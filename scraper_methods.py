import requests
from bs4 import BeautifulSoup
import re
import logging
import time
from datetime import datetime
import pandas as pd
import praw

class Dto(object):
    pass

def log_err(err):
    #TODO: add logging functionality
    return err

def set_dto():
    #get the date/time and format date and time stamps
    dtobj = Dto()
    dtobj.timestr = str(time.time())
    dtobj.dto=datetime.now(tz=None)
    dtobj.date = str(dtobj.dto.year)+'/'+str(dtobj.dto.month)+'/'+str(dtobj.dto.day)
    dtobj.time = str(dtobj.dto.hour)+': '+str(dtobj.dto.minute)+':'+str(dtobj.dto.second)
    return dtobj

def connect(id, secret, agent):
    return praw.Reddit(client_id=id, client_secret=secret, user_agent=agent)

def serialize_poll(poll_data):
    poll = 'num_votes:'+str(poll_data.total_vote_count)+';'
    i=0
    for option in poll_data.options:
        i=i+1
        poll+='option'+str(i)+'_text:'+str(option.text)+';'
    return poll

def scrape_subreddit(reddit,dto,subreddit,mode,plimit,submission_params,comment_params,tfilt,csv_path):
    posts=[]
    comments = []
    i = 0
    date = dto.date
    time = dto.time
    timestr = dto.timestr

    if mode=='hot':
        post_list = reddit.subreddit(subreddit).hot(limit=plimit)
    elif mode=='top':
        post_list = reddit.subreddit(subreddit).top(limit=plimit,time_filter=tfilt)
    elif mode=='new':
        post_list = reddit.subreddit(subreddit).new(limit=plimit)
    elif mode=='rising':
        post_list = reddit.subreddit(subreddit).rising(limit=plimit)
    else:
        log_err('ERROR 00: invalid mode provided')
        return 1
    
    #gather post data
    for post in post_list:
        #limit top posts gathered if plimit is set
        i += 1
        if(mode=='top' and plimit and i > plimit):
            break

        #pull a row of data, getting properties from submission_params
        pull_params = [date, time]
        for param in submission_params:
            if(param.var=='poll_data'):
                if(hasattr(post,'poll_data')):
                    pull_params.append(serialize_poll(post.poll_data))
                else:
                    pull_params.append('N/A')
            elif(param.var=='link_flair_template_id'):
                if(hasattr(post,'link_flair_template_id')):
                    pull_params.append(post.link_flair_template_id)
                else:
                    pull_params.append('N/A')
            else:
                pull_params.append(getattr(post,param.var))
        posts.append(pull_params)

        #pull the comment tree attached to the post
        submission = reddit.submission(id=post.id)
        submission.comments.replace_more(limit=0)
        for comment in submission.comments.list():
            comments_pull_params = [date, time]
            for param in comment_params:
                comments_pull_params.append(getattr(comment,param.var))
            comments.append(comments_pull_params)
    opt_str = ''
    if(plimit or tfilt):
        opt_str += '_'
    if(plimit):
        opt_str += 'lim='+str(plimit)+'_'
    if(tfilt and mode=='top'):
        opt_str += 'rng='+str(tfilt)+'_'
    cols = ['sample_date', 'sample_time']
    for col_hd in submission_params:
        cols.append(col_hd.header)
    posts = pd.DataFrame(posts,columns=cols)
    posts.to_csv(csv_path+timestr+'_rdt_'+subreddit+'_'+mode+opt_str+'_posts.csv')
    comments = pd.DataFrame(comments,columns=['sample_date', 'sample_time', 'post_id', 'parent_id', 'comment_id', 'url', 'score', 'author', 'body', 'body_html','created','distinguished?','edited?','is OP?','is stickied?'])
    comments.to_csv(csv_path+timestr+'_rdt_'+subreddit+'_'+mode+opt_str+'_comments.csv')
    return 0

def scrape_pdx_forum(forum_ext,dto,thread_limit,csv_path):
    #configure logging
    logging.basicConfig(filename='log.log',filemode='w',format='%(asctime)s %(message)s',level=logging.DEBUG)
    logging.info('start')

    #configure initial parameters
    forum = 'ck3'
    forum_src = 'crusader-kings-iii'
    date = dto.date
    time = dto.time
    timestr = dto.timestr
    cols = ['sample date','sample time'] + ['url','id','forum','title','author','replies','views','created on','last updated','body']
    c_cols = ['sample date','sample time','thread id','comment id','comment author','body','replied on','reactions']
    delays = [0.8,0.81,0.82,0.83,0.84,0.85,0.86,0.87,0.88,0.89,0.9,0.91,0.92,0.93,0.94,0.95,0.96,0.97,0.98,0.99,1,1.01,1.02,1.03,1.04,1.05,1.06,1.07,1.08,1.09,1.1,1.11,1.12,1.13,1.14,1.15,1.16,1.17,1.18,1.19,1.2]
    page_num = 0
    today_only = 1
    thread_data = []
    comment_data = []
    base_url = 'https://forum.paradoxplaza.com'
    f_url = base_url + forum_ext
    t_num = 0

    while today_only and t_num <= thread_limit:
        #calculate pagination component of url
        page_num += 1
        if(page_num > 1):   url = f_url+'page-'+str(page_num)
        else:               url = f_url

        #get the page of threads
        f_page = requests.get(url)
        f_soup = BeautifulSoup(f_page.text, 'html.parser')

        #get the main content area
        threads = f_soup.find('div',['uix_contentWrapper']).find('div',['structItemContainer-group js-threadList']).find_all('div','structItem')
        if threads:
            for thread in threads:
                t_num += 1
                if t_num > thread_limit: break

                #get the "last updated" timestamp; if it's greater than 24 hours, stop pulling data and jump to logging
                t_updated = thread.find('div',['structItem-cell structItem-cell--latest']).find('time')
                if t_updated: 
                    upd = datetime.fromtimestamp(int(t_updated.get('data-time')), tz=None)
                    delta = dto.dto-upd
                    if delta.days > 0: 
                        today_only = 0
                        break
                #prepend date and time
                t_params = [date,time]
                #get url
                t_title = thread.find('div',['structItem-title']).find('a')
                if t_title: t_url = t_title.get('href')
                else:       t_url ="N/A"
                t_params.append(t_url)
                #parse id
                t_id = re.search("\.(\d*)", t_url).group(1)
                t_params.append(t_id)
                #append forum name
                t_params.append(forum_src)
                #get title
                if t_title:t_params.append(t_title.get_text())
                else:           t_params.append("N/A")
                #get author
                t_author = thread.find('a',['username'])
                if t_author:t_params.append(t_author.get_text())
                else:t_params.append("N/A")
                #get views/replies
                meta = thread.find('div',['structItem-cell structItem-cell--meta'])
                if meta:
                    t_replies = meta.find('dl',['pairs pairs--justified']).find('dd')
                    t_views = meta.find('dl',['pairs pairs--justified structItem-minor']).find('dd')
                    if t_replies:t_params.append(re.sub('\s+',' ',t_replies.get_text()))
                    else:t_params.append("N/A")
                    if t_views: t_params.append(t_views.get_text())
                    else:t_params.append("N/A")
                else: 
                    t_params.append("N/A")
                    t_params.append("N/A")
                #get the post creation time
                creation = thread.find('li',['structItem-startDate']).find('time')
                if creation: t_params.append(creation.get('datetime'))
                else: t_params.append("N/A")
                #append the "last updated" timestamp
                if t_updated: 
                    t_params.append(t_updated.get('datetime'))
                else: t_params.append("N/A")
                

                #get the thread's comments
                if(t_url != "N/A"):
                    t_url = 'https://forum.paradoxplaza.com'+str(t_url)
                    t_page = requests.get(t_url)
                    t_soup = BeautifulSoup(t_page.text, 'html.parser')
                    comments = t_soup.find('div',['block-body js-replyNewMessageContainer'])
                    if(comments):
                        comments = comments.find_all('article','message')
                        #sort through each comment in the thread
                        c_num = 0
                        for comment in comments:
                            c_num += 1
                            if c_num==1:
                                t_body = comment.find('div',['bbWrapper']).get_text()
                                if(t_body): t_params.append(t_body)
                                else:       t_params.append("N/A")

                            #prepend date and time
                            c_params = [date,time,t_id]
                            #get comment's id
                            c_id = comment.get('data-content')
                            if c_id:    c_params.append(c_id)
                            else:       c_params.append("N/A")
                            c_auth = comment.get('data-author')
                            #get comment's author
                            if c_auth:  c_params.append(c_auth)
                            else:       c_params.append("N/A")
                            #get comment's main text
                            c_body = comment.find('div',['bbWrapper']).get_text()
                            if(c_body): c_params.append(c_body)
                            else:       c_params.append("N/A")
                            #get comment's creation time
                            c_creation = comment.find('time')
                            if creation: c_params.append(c_creation.get('datetime'))
                            else: c_params.append("N/A")
                            #get comment's reactions
                            c_reacts = {}
                            reactions = comment.find('ul',['reactionSummary'])
                            if reactions:
                                reactions = reactions.find_all('li')
                                for reaction in reactions:
                                    r_type =    reaction.find('span',['th_reactplus_reaction-identifier'])
                                    if(r_type): r_type = r_type.get('title')
                                    r_count =   reaction.find('span',['th_reactplus_reaction-count']).get_text()
                                    if r_type and r_count:  c_reacts[r_type] = r_count
                            c_params.append(c_reacts)
                            #finally load the comment's data into the 2D data array
                            comment_data.append(c_params)

                
                #append this thread's info to the array of thread data
                thread_data.append(t_params)

        
        #guardrail; if more than 50 pages are updated within a day, something crazy is happening!
        if page_num > 50:
            today_only=0
            logging.info('WARNING: >50 pages logged in 1 segment')

    #output csv and log completion
    thread_output = pd.DataFrame(thread_data,columns=cols)
    thread_output.to_csv(csv_path+timestr+'_pdx_'+forum+'_threads.csv')
    thread_output = pd.DataFrame(comment_data,columns=c_cols)
    thread_output.to_csv(csv_path+timestr+'_pdx_'+forum+'_comments.csv')
    logging.info('printed %s items to _posts.csv' % len(thread_data))
    return 0
