from bs4 import BeautifulSoup
import opener, configs, re, sys, socket, random, math,time, multiprocessing, threading, marshal
#import logging
from urlparse import urlparse
from base64 import standard_b64decode
import MySQLdb as mdb
from datetime import datetime
from multiprocessing import Pool
import urllib2


socket.setdefaulttimeout(15)
WORKER = 2 # Number of processor = 4
THREAD_NUM = 5
THREAD_NUM_PER_PRO = 3
TEST = 0
#LOG_FILE='1channel.log'
#logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG,)

def get_title(soup):
	title=''
	title = soup(attrs={"property":"og:title"})[0]['content'].strip()
	return title

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
	



def i_have_got_series_link(url, con):
	#~ print 'Series:%s#%s' %(name,url)
	data=opener.fetch(url)['data']
	soup=BeautifulSoup(data, 'lxml')
	name = get_title(soup)
	if len(name) == 0:
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
			imdb_id = re.search(r'\d+', imdb_link)
	except:
		pass
	
	series_id_in_database=get_series_id_in_database(name,imdb_id,released_date,con)
	
	#getting all episodes
	all_eps=soup(attrs={'class':'tv_episode_item'})
		
	# getting all eps except the transparent one 		
	all_eps[:] = [base+x('a')[0].get('href') if not 'transp2' in x['class'] else None for x in all_eps ]

	all_eps = list(set(all_eps))
	if None in all_eps:
		all_eps.remove(None)
	
	for ep_link in all_eps:
		matches = re.search(r'season-(\d+)-episode-(\d+)', ep_link)
		season = int(matches.group(1))
		episode = int(matches.group(2))
		i_have_got_series_episode_url(name,series_id_in_database,ep_link,season,episode,con)
	



		

def get_all_series_links(pages):
	(all_series, counter)=([], 0)
	
	for url in pages:
		counter+=1
		if TEST:
			if counter==2:
				break
		data=opener.fetch(url)['data']
		#~ url_to = '%s.html'%i
		#~ f=open(url_to,'w')
		#~ f.write(data)
		#~ return
		soup=BeautifulSoup(data, 'lxml')
		l=soup.find_all('a')
		reg=re.compile(r'.*/watch-\d+-(.*)')
		for i in l:
			if not i.has_key('href'):
				continue;
			if not i.has_key('title'):
				continue;
			link =i.get('href')
			m=reg.match(link)
			if m:
				series_name=i.get('title')
				if 'Watch' in series_name:
					series_name=series_name[6:-7].strip()
				series_link="http://www.1channel.ch"+m.group(0)
				all_series.append((series_name, series_link))
	
	return all_series
	
	
def get_all_series_links_thread(tot):
	
	#print 'inside a thread@@@@@@@@@@@@@!!!!!!!!!!!!'
	counter=0
	for url in tot:
		#print 'getting from: %s' % url
		counter+=1
		if TEST:
			if counter==2:
				break
		data=opener.fetch(url)['data']
		#~ url_to = '%s.html'%i
		#~ f=open(url_to,'w')
		#~ f.write(data)
		#~ return
		soup=BeautifulSoup(data,'lxml')
		l=soup.find_all('a')
		reg=re.compile(r'.*/watch-\d+-(.*)')
		for i in l:
			if not i.has_key('href'):
				continue;

			link =i.get('href')
			m=reg.match(link)
			if m:
				#~ series_name=i.get('title')
				#~ if 'Watch' in series_name:
					#~ series_name=series_name[6:-7].strip()
				series_link="http://www.1channel.ch"+m.group(0)
				all_series.append(series_link)
	
	
	

	
def start_threads_in_process(series_link):
	#~ series_name=re.sub("&#(\d+)(;|(?=\s))", _callback, series_name)
	#~ con=mdb.connect(configs.HOST,configs.USER,configs.PASS, configs.DB, charset='utf8')
	#~ log_id=uuid.uuid4()
	#~ LOG_FILENAME='/home/vid/1channnel%s.log' % log_id
	#~ logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,)

	#print 'Process started: %s' % multiprocessing.current_process()
	con=mdb.connect(configs.HOST,configs.USER,configs.PASS, configs.DB, charset='utf8')
	try:
		i_have_got_series_link(series_link, con)
	except:
		print 'exception in %s ' % series_link
		raise
	#print 'Process ended: %s' % multiprocessing.current_process()
	con.close()

# sending one by one to the thread
def ini_thread(series_list):
	for i in series_list:
		#if random.randint(0,1) == 1:
			#time.sleep(2)

		start_threads_in_process(i)
	#con.close()	
	
	# starting multiprocess
def start_multiprocessing_with_threads(chunk_list):
	print ' process started, chunk size: %s' % len(chunk_list)
	chunk_thread_size = (len(chunk_list)+THREAD_NUM_PER_PRO-1)/THREAD_NUM_PER_PRO
	chunks_for_process_threads=chunks(chunk_list, chunk_thread_size)
	for i in range(THREAD_NUM_PER_PRO):
		#~ get_all_series_links_thread(thread_chunks[i])
		#~ continue
		thread = threading.Thread(target=ini_thread, args=(chunks_for_process_threads[i],))
		thread.start()
		threads.append(thread)
	for thread in threads:
		thread.join()

	

def generate_all_the_main_page_name():
	l=[]
	l.append('http://www.1channel.ch/?letter=123&tv')
	#~ if TEST:
		#~ return l
	for i in range(ord('a'),ord('z')+1):
		l.append('http://www.1channel.ch/?letter='+str(chr(i))+'&tv')
	
	#generating all pages with page number	
	all_pages = [] 
	for url in l:
		page_count=1
		data=opener.fetch(url)['data']
		soup=BeautifulSoup(data,'lxml')
		l=soup.select('.pagination > a ')		
		
		if len(l) != 0:
			ref = l[len(l)-1]['href']
			reg=re.compile(r'.*?page=(\d+)')
			#~ print url, l[len(l)-1]['href']
			m=reg.match(ref)
			if m:
				page_count=int(m.group(1))
				#~ print page_count
		for i in range(1,page_count+1):
			all_pages.append(url+"=&page="+str(i))
	return all_pages

def generate_main_pages_thread(url):
	#print url
	page_count=1
	data=opener.fetch(url)['data']
	soup=BeautifulSoup(data, 'lxml')
	l=soup.select('.pagination > a ')		
		
	if len(l) != 0:
		ref = l[len(l)-1]['href']
		reg=re.compile(r'.*?page=(\d+)')
		#~ print url, l[len(l)-1]['href']
		m=reg.match(ref)
		if m:
			page_count=int(m.group(1))
			#~ print page_count
	for i in range(1,page_count+1):
		tot.append(url+"=&page="+str(i))
	
	
def chunks(l, n):
	return [l[i:i+n] for i in range(0, len(l), n)]

def _callback(matches):
    id = matches.group(1)
    try:
        return unichr(int(id))
    except:
        return id


if __name__=='__main__':
	base = 'http://www.1channel.ch'
	
	#~ print generate_all_the_main_page_name()	
	#~ i_have_got_series_name("http://www.1channel.ch/watch-9460-2020","hello")
	#~ i_have_got_page_number('http://www.1channel.ch/?letter=123&tv&page=1')
	#~ get_page_count_and_go_deeper('http://www.1channel.ch/?letter=123&tv')
	
	
	#Generating all the page links using threading
	
	t1=time.time()
	pages = []
	tot=[]
	
	pages.append('http://www.1channel.ch/?letter=123&tv')
	for i in range(ord('a'),ord('z')+1):
		pages.append('http://www.1channel.ch/?letter='+str(chr(i))+'&tv')
	
	
	
	thread_pages=[]
	for i in range(len(pages)):
		threadp = threading.Thread(target=generate_main_pages_thread, args=(pages[i],))
		threadp.start()
		thread_pages.append(threadp)
		
	for thread in thread_pages:
		thread.join()
		
	print 'initial Threading done PAGES FOUND %s ' % len(tot)
	print 'Time to get ALl Pages: %s Seconds' %(time.time()-t1)
	#~ tot = generate_all_the_main_page_name()
	random.shuffle(tot)
	
	# Generating all the series links using threading
	all_series=[]
	if len(tot)>THREAD_NUM:
		thread_numbers=THREAD_NUM
	else:
		thread_numbers=1
		
	chunk_length = int(math.ceil(len(tot)/THREAD_NUM))
	if chunk_length == 0:
		chunk_length = 1
	thread_chunks=chunks(tot, chunk_length )
	threads=[]
	
	print 'Pages split into: %s' % len(thread_chunks)
	
	for i in range(thread_numbers):
		#~ get_all_series_links_thread(thread_chunks[i])
		#~ continue
		thread = threading.Thread(target=get_all_series_links_thread, args=(thread_chunks[i],))
		thread.start()
		threads.append(thread)
	for thread in threads:
		thread.join()
	print 'threading done'
	print 'TOTAL SERIES %s' % len(all_series)

	print 'Time to get ALl Series: %s Seconds' %(time.time()-t1)
		
	#~ all_series=get_all_series_links(tot)		
	random.shuffle(all_series)
	#default encoding ASCII error proned
	#~ all_series=[(s.encode('ascii', 'xmlcharrefreplace'),i) for s,i in all_series ]
	#~ print all_series[0]
	#sys.exit()
	#~ sys.exit()
	#~ print 'Total pages to loop over: %s' % len(tot)
	#~ print 'Total Series to loop over: %s' % len(all_series)
	#~ print tot
	#~ print len(tot)
	#~ sys.exit()
	
	# Starting multiprocessing
	p=Pool(processes=WORKER)
	chunk_size = (len(all_series)+WORKER-1)/WORKER
	print 'Chunk Size set to: %s' % chunk_size	
	
	series_to_process = chunks(all_series, chunk_size)
	
	#try:
#	for i in all_series:
#		start_multiprocessing(i)
	p.map(start_multiprocessing_with_threads , series_to_process)
	t2 = time.time()
	t = t2-t1
	#Test with saved series in the file marshalled
	#~ f=open('marshalled')
	#~ all_series=marshal.load(f)
	#~ all_series= all_series[:20]
	#~ for i in all_series:
		#~ start_threads_in_process(i)
#	except:
#		print 'Process Allocation Error^^^^^^^^^^^^^^^^^'
	
	print '%s Hours %s Seconds' %(int(t/3600),t%3600)
		
	#~ p.map_async(get_page_count_and_go_deeper,tot)
	
