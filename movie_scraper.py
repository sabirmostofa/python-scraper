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
WORKER = 3 # Number of processor = 4
THREAD_NUM = 20
THREAD_NUM_PER_PRO = 2
TEST = 0
#LOG_FILE='1channel.log'
#logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG,)


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

def insert_into_links_table(movies_id, link, con):
	
	cur=con.cursor()
	cur.execute('SELECT * FROM `vs_links` WHERE `movies_id`=%s AND `link_url`=%s',(movies_id,link))
	res=cur.fetchone()
	if res:
		return
	else :
		sql='''INSERT ignore INTO `vs_links`(`movies_id`, `link_url`) VALUES (%s,%s)'''
		args=(movies_id,link,)
		cur.execute(sql,args)





def i_have_got_movies_name((url,name,con)):
	print 'movie:%s#%s' %(name,url)
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
			imdb_id = re.search(r'\d+', imdb_link)
	except:
		pass
	
	movies_id_in_database=get_movies_id_in_database(name,imdb_id,released_date,con)
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
	







		
#~ 
def get_all_movies_links(pages):
	(all_movies, counter)=([], 0)
	
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
				movies_name=i.get('title')
				if 'Watch' in movies_name:
					movies_name=movies_name[6:-7].strip()
				movies_link="http://www.1channel.ch"+m.group(0)
				all_movies.append((movies_name, movies_link))
	
	return all_movies
	
	
def get_all_movies_links_thread(tot):
	
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
			if not i.has_key('title'):
				continue;
			link =i.get('href')
			m=reg.match(link)
			if m:
				movie_name=i.get('title')
				if 'Watch' in movie_name:
					movie_name=movie_name[6:-7].strip()
				movie_link="http://www.1channel.ch"+m.group(0)
				all_movies.append((movie_name, movie_link))
	
	
	
	
	# starting multiprocess
def start_multiprocessing((movies_name, movies_link)):
	movies_name=re.sub("&#(\d+)(;|(?=\s))", _callback, movies_name)
	con=mdb.connect(configs.HOST,configs.USER,configs.PASS, configs.DB, charset='utf8')
	#~ log_id=uuid.uuid4()
	#~ LOG_FILENAME='/home/vid/1channnel%s.log' % log_id
	#~ logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,)

	#print 'Process started: %s' % multiprocessing.current_process()
	try:
		i_have_got_movies_name((movies_link,movies_name,con))
	except:
		#logging.exception('Got exception in movies: %s' % movies_name)
		raise
	#print 'Process ended: %s' % multiprocessing.current_process()
	con.close()
	# starting multiprocess
	
def start_threads_in_process((movies_name, movies_link)):
	movies_name=re.sub("&#(\d+)(;|(?=\s))", _callback, movies_name)
	#~ con=mdb.connect(configs.HOST,configs.USER,configs.PASS, configs.DB, charset='utf8')
	#~ log_id=uuid.uuid4()
	#~ LOG_FILENAME='/home/vid/1channnel%s.log' % log_id
	#~ logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,)

	#print 'Process started: %s' % multiprocessing.current_process()
	con=mdb.connect(configs.HOST,configs.USER,configs.PASS, configs.DB, charset='utf8')
	try:
		i_have_got_movies_name((movies_link,movies_name,con))
	except:
		#logging.exception('Got exception in movies: %s' % movies_name)
		raise
	#print 'Process ended: %s' % multiprocessing.current_process()
	con.close()

# sending one by one to the thread
def ini_thread(movies_list):
	for i in movies_list:
		#~ if random.randint(0,1) == 1:
			#~ time.sleep(2)
		start_threads_in_process(i)
	#con.close()	
	
	# starting multiprocess
def start_multiprocessing_with_threads(chunk_list):
	print ' process started, chunk size: %s' % len(chunk_list)
	chunk_thread_size = (len(chunk_list)+THREAD_NUM_PER_PRO-1)/THREAD_NUM_PER_PRO
	chunks_for_process_threads=chunks(chunk_list, chunk_thread_size)
	for i in range(THREAD_NUM_PER_PRO):
		#~ get_all_movies_links_thread(thread_chunks[i])
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
		l.append('http://www.1channel.ch/?letter='+str(chr(i)))
	
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
	
	#~ print generate_all_the_main_page_name()	
	#~ i_have_got_movies_name("http://www.1channel.ch/watch-9460-2020","hello")
	#~ i_have_got_page_number('http://www.1channel.ch/?letter=123&tv&page=1')
	#~ get_page_count_and_go_deeper('http://www.1channel.ch/?letter=123&tv')
	
	
	#Generating all the page links using threading
	
	t1=time.time()
	pages = []
	tot=[]
	pages.append('http://www.1channel.ch/?letter=123')
	for i in range(ord('a'),ord('z')+1):
		pages.append('http://www.1channel.ch/?letter='+str(chr(i)))
	
	#debug val
	pages= ['http://www.1channel.ch/?letter=123']
	thread_pages=[]
	for i in range(len(pages)):
		threadp = threading.Thread(target=generate_main_pages_thread, args=(pages[i],))
		threadp.start()
		thread_pages.append(threadp)
		
	for thread in thread_pages:
		thread.join()
		
	print 'initial Threading done PAGES FOUND %s ' % len(tot)
	#~ tot = generate_all_the_main_page_name()
	random.shuffle(tot)
	#debug val
	tot=[tot[0]]
	# Generating all the movies links using threading
	all_movies=[]
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
		#~ get_all_movies_links_thread(thread_chunks[i])
		#~ continue
		thread = threading.Thread(target=get_all_movies_links_thread, args=(thread_chunks[i],))
		thread.start()
		threads.append(thread)
	for thread in threads:
		thread.join()
	print 'threading done'
	print 'TOTAL movies %s' % len(all_movies)
		
	#~ all_movies=get_all_movies_links(tot)		
	random.shuffle(all_movies)
	#default encoding ASCII error proned
	#~ all_movies=[(s.encode('ascii', 'xmlcharrefreplace'),i) for s,i in all_movies ]
	#~ print all_movies[0]
	#sys.exit()
	#~ sys.exit()
	#~ print 'Total pages to loop over: %s' % len(tot)
	#~ print 'Total movies to loop over: %s' % len(all_movies)
	#~ print tot
	#~ print len(tot)
	#~ sys.exit()
	# Starting multiprocessing
	p=Pool(processes=WORKER)
	chunk_size = (len(all_movies)+WORKER-1)/WORKER
	print 'Chunk Size set to: %s' % chunk_size	
	
	movies_to_process = chunks(all_movies, chunk_size)
	print(len(movies_to_process))
	print movies_to_process
	#try:
#	for i in all_movies:
#		start_multiprocessing(i)
	p.map(start_multiprocessing_with_threads , movies_to_process)
	t2 = time.time()
	t = t2-t1
	print '%s Hours %s Seconds' %(int(t/3600),t%3600)
	#~ f=open('marshalled')
	#~ all_movies=marshal.load(f)
	#~ for i in all_movies:
		#~ start_multiprocessing(i)
#	except:
#		print 'Process Allocation Error^^^^^^^^^^^^^^^^^'
	
		
	#~ p.map_async(get_page_count_and_go_deeper,tot)
	
