from scraper_methods import *
from collections import namedtuple

#connect to reddit's api
id = "nyWwqpBYy6bGlY3cM1D8Jw"
secret="P-ctGYukCt8rUKkfryxR54i7G9U5XQ"
agent="research_scraper"
reddit = connect(id, secret, agent)

#configure desired reddit parameters
Param = namedtuple("Param","var header")
rdt_sub_params = [Param('id','id'),Param('subreddit','subreddit'),Param('title','title'),Param('url','res_url'),Param('permalink','url'),Param('score','score'),Param('author','author'),Param('num_comments','num_comments'),Param('selftext','body'),Param('created_utc','created'),Param('distinguished','distinguished'),Param('edited','edited'),Param('is_original_content','is OC?'),Param('is_self','is self'),Param('link_flair_template_id','link flair template id'),Param('link_flair_text','link flair text'),Param('locked','locked'),Param('over_18','over 18'),Param('saved','saved'),Param('spoiler','spoiler'),Param('stickied','stickied'),Param('upvote_ratio','upvote ratio'),Param('poll_data','poll')]
rdt_com_params = [Param('link_id','post id'),Param('parent_id','parent id'),Param('id','comment id'),Param('permalink','url'),Param('score','score'),Param('author','author'),Param('body','body'),Param('body_html','html'),Param('created_utc','created'),Param('distinguished','dinstinguished'),Param('edited','edited'),Param('is_submitter','is submitter'),Param('stickied','stickied')]

#configure desired pdx parameters
#pdx_forum_ext = '/forum/forums/crusader-kings-iii.1059/'

#set date/time data
dto=set_dto()

#scrape the daily selection
#scrape_subreddit(reddit,dto,'paradoxplaza'  ,'top',10,rdt_sub_params,rdt_com_params,'day','daily_select_csv/')
#scrape_subreddit(reddit,dto,'crusaderkings3','top',10,rdt_sub_params,rdt_com_params,'day','daily_select_csv/')
scrape_subreddit(reddit,dto,'CrusaderKings' ,'top',10,rdt_sub_params,rdt_com_params,'day','daily_select_csv/')
#scrape_pdx_forum(pdx_forum_ext,dto,10,'daily_select_csv/')
