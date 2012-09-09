from bs4 import BeautifulSoup
import opener, configs, re, sys, socket, random, math,time
from urlparse import urlparse
from base64 import standard_b64decode
import MySQLdb as mdb
from datetime import datetime
import urllib2


socket.setdefaulttimeout(15)
WORKER = 3 # Number of processor = 4
THREAD_NUM = 20
THREAD_NUM_PER_PRO = 7
TEST = 0
#LOG_FILE='1channel.log'
#logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG,)

def get_title(soup):
	title=''
	title = soup(attrs={"property":"og:title"})[0]['content'].strip()
	return title

def get_series_id(name):
	cur=con.cursor(mdb.cursors.DictCursor)
	cur.execute("SELECT * FROM `vs_series` WHERE `series_name`=%s",name)
	res=cur.fetchone()
	if res:
		return res['series_id']

# functions for movies
def get_movies_id_in_database(name,link,d,con):
	
	cur=con.cursor(mdb.cursors.DictCursor)
	cur.execute("SELECT * FROM `vs_movies` WHERE `movie_name`=%s",name)
	res=cur.fetchone()
	if res:
		return res['movie_id']
	else :
		sql='''INSERT ignore INTO `vs_movies`(`movie_name`, `movie_channel_link`, `movie_release_date`) VALUES (%s,%s,%s)'''
		args=(name,link,d)
		cur.execute(sql,args)
		return get_movies_id_in_database(name,link,d,con)

def insert_into_links_table(movie_id, link, con):
	
	cur=con.cursor()
	cur.execute('SELECT * FROM `vs_links` WHERE `movie_id`=%s AND `link_url`=%s',(movie_id,link))
	res=cur.fetchone()
	if res:
		return
	else :
		sql='''INSERT ignore INTO `vs_links`(`movie_id`, `link_url`) VALUES (%s,%s)'''
		args=(movie_id,link,)
		cur.execute(sql,args)
		

#prepares data for db insertion and calls 
def i_have_got_movies_url(url):
	
	data=opener.fetch(url)['data']
	soup=BeautifulSoup(data, 'lxml')
	
	released_date=datetime.today()
	
	try:
		l=soup.select('.movie_info > table ')
		l=(l[0].find_all('tr'))
		l=l[1].find_all('td')[1].text
		released_date=datetime.strptime(l,'%B %d, %Y')
	except:
		pass
	
	imdb_id="-1"
	try:
		imdb_link=soup.select('.mlink_imdb')[0].find_all('a')
		imdb_link=imdb_link[0].get('href')
		if re.search(r'\d+', imdb_link):
			imdb_id = re.search(r'\d+', imdb_link).group(0)
	except:
		pass
		
	name = get_title(soup)
	
	if len(name) == 0:
		return
	
	movie_id=get_movies_id_in_database(name,imdb_id,released_date,con)
	l=soup.find_all('a')
	reg=re.compile(r'.*?url=(.+?)&domain.*')
	reg2=re.compile(r'.*external.php.*')
	
	for i in l:
		if not i.has_key('href'):
			continue
		ref=i['href']
		parsed=urlparse(ref)
		try:
			t1=parsed[2]
			if not reg2.match(t1):
				continue
			m=reg.match(parsed[4])
			final_url=standard_b64decode(m.group(1))
			insert_into_links_table(movie_id,final_url, con)
		except:
			pass
	
	
# functions for series
def get_series_id_in_database(name,link,d,con):
	
	cur=con.cursor(mdb.cursors.DictCursor)
	cur.execute("SELECT * FROM `vs_series` WHERE `series_name`=%s",name)
	res=cur.fetchone()
	if res:
		return res['series_id']
	else :
		sql='''INSERT ignore INTO `vs_series`(`series_name`, `imdb_link`, `series_release_date`) VALUES (%s,%s,%s)'''
		args=(name,link,d)
		cur.execute(sql,args)
		return get_series_id_in_database(name,link,d,con)

def set_season_episode(series_id,episode_link,season,episode,con):
	
	cur=con.cursor()
	cur.execute('SELECT * FROM `vs_series_links` WHERE `series_id`=%s AND `link_url`=%s',(series_id,episode_link))
	res=cur.fetchone()
	if res:
		return
	else :
		sql='''INSERT ignore INTO `vs_series_links`(`series_id`, `season`, `episode`, `link_url`) VALUES (%s,%s,%s,%s)'''
		args=(series_id,season,episode,episode_link,)
		cur.execute(sql,args)

def i_have_got_series_episode_url(name,series_id,url,season,episode,con):
	#print '\n\nName: %s\nSeason: %s .Episode: %s' %(name,season,episode)
	#data=urllib2.open(url).read()
	
	data=opener.fetch(url)['data']
	soup=BeautifulSoup(data, 'lxml')
	l=soup.find_all('a')
	reg=re.compile(r'.*?url=(.+?)&domain.*')
	reg2=re.compile(r'.*external.php.*')
	
	for i in l:
		if not i.has_key('href'):
			continue
		ref=i['href']
		parsed=urlparse(ref)
		try:
			t1=parsed[2]
			if not reg2.match(t1):
				continue
			m=reg.match(parsed[4])
			final_url=standard_b64decode(m.group(1))
			set_season_episode(series_id,final_url,season,episode,con)
		except:
			pass


	
	
def fetch_prime_time_episodes(soup):
	links = soup(attrs={'id':'slide-runner'})[0]('a')	
	links[:] = [base+x.get('href') for x in links if x.has_attr('title')]	
	return links
			
	
def get_latest_ones(soup):	
	show_divs = soup(attrs={'class':'index_item index_item_ie'})
	show_links = [ base+x('a')[0].get('href') for x in show_divs]
	return show_links

def get_put_unique_eps(url='show.html'):
	#~ print url
	data=opener.fetch(url)['data']
	soup = BeautifulSoup(data, 'lxml')
	title= get_title(soup)
	if len(title) == 0:
		#~ print 'title is null returning'
		return
	
	released_date=datetime.today()
	
	try:
		l=soup.select('.movie_info > table ')
		l=(l[0].find_all('tr'))
		l=l[1].find_all('td')[1].text
		released_date=datetime.strptime(l,'%B %d, %Y')
	except:
		pass
	
	imdb_id="-1"
	try:
		imdb_link=soup.select('.mlink_imdb')[0].find_all('a')
		imdb_link=imdb_link[0].get('href')
		if re.search(r'\d+', imdb_link):
			imdb_id = re.search(r'\d+', imdb_link).group(0)
	except:
		pass
	
	series_id = get_series_id_in_database(title, imdb_id, released_date, con)
	all_eps=soup(attrs={'class':'tv_episode_item'})

	
	# getting all eps except the transparent one 		
	all_eps[:] = [base+x('a')[0].get('href') if not 'transp2' in x['class'] else None for x in all_eps ]

	all_eps = list(set(all_eps))
	if None in all_eps:
		all_eps.remove(None)

	
	

	
	cur=con.cursor(mdb.cursors.DictCursor)
	for link in all_eps:
		#~ print 'episode link: %s' % link
		matches = re.search(r'season-(\d+)-episode-(\d+)', link)
		season = int(matches.group(1))
		episode = int(matches.group(2))		
		cur.execute("SELECT * FROM `vs_series_links` WHERE `series_id`=%s and `season`=%s and `episode`=%s" ,
		(series_id, season, episode))
		if cur.fetchone():
			continue
		#~ print 'Inserting new Episode: series: %s season: %s episode: %s' % (series_id, season, episode)
		i_have_got_series_episode_url(title, series_id, link, season, episode, con)
		
	
def put_prime_time_eps(link):
	data=opener.fetch(link)['data']
	soup = BeautifulSoup(data, 'lxml')
	title = get_title(soup)
	if len(title) == 0:
		return
		
	series_id = get_series_id(title)
	if not series_id:
		series_link = base + soup(attrs = {'class':'titles'})[1]('a')[0]['href']
		get_put_unique_eps(series_link)
		return
		
	 
	matches = re.search(r'season-(\d+)-episode-(\d+)', link)
	season = int(matches.group(1))
	episode = int(matches.group(2))
	i_have_got_series_episode_url(title, series_id, link, season, episode, con)
	
		

def initiator():
	tv_url = 'http://www.1channel.ch/?tv'
	featured = 'http://www.1channel.ch/index.php?sort=featured'
	data=opener.fetch(tv_url)['data']
	soup = BeautifulSoup(data, 'lxml')

	
	#Inserting latest shows
	latest_shows =  get_latest_ones(soup)
	#~ print latest_shows
	for show in latest_shows:
		get_put_unique_eps(show)
		
	# episodes prime times
	eps =[]	
	eps = fetch_prime_time_episodes(soup)
	for epi in eps:
		put_prime_time_eps(epi)
		
	#insert movies
	del data, soup
	data=opener.fetch(base)['data']
	soup = BeautifulSoup(data, 'lxml')
	latest_movies = get_latest_ones(soup)

	
	#check featured
	del data, soup
	data=opener.fetch(featured)['data']
	soup = BeautifulSoup(data, 'lxml')
	featured_movs = get_latest_ones(soup)
	
	to_parse = set(latest_movies+featured_movs)
	#~ print 'Parsing movies: %s ' % len(to_parse)
	
	for url in to_parse:
		i_have_got_movies_url(url)


if __name__=='__main__':
	base = 'http://www.1channel.ch'
	con=mdb.connect(configs.HOST,configs.USER,configs.PASS, configs.DB, charset='utf8')
	cycle = 0
	#~ get_put_unique_eps('transp.html')
	while 1:
		cycle+=1
		pre= time.time()
		initiator()
		print 'Cycle: %s  time: %s' % (cycle,  time.time()-pre )
		time.sleep(600)

	

	
	

	
