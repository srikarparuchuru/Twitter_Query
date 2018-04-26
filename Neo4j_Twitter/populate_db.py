import json
from py2neo import Graph
from py2neo import Node,Relationship
from json import dumps

graph = Graph()

graph.run("MATCH (n) DETACH DELETE n")

# graph.run("CREATE CONSTRAINT ON (u:User) ASSERT u.screen_name IS UNIQUE")
# graph.run("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.tid IS UNIQUE")
# graph.run("CREATE CONSTRAINT ON (l:Location) ASSERT l.place IS UNIQUE")

graph.run("CREATE INDEX ON :User(name)")
graph.run("CREATE INDEX ON :Tweet(tid)")
graph.run("CREATE INDEX ON :Location(place)")

stm = "MERGE (u:User {screen_name:{U3}}) SET u.id={U2},u.name={U1},u.image={U4}" 
stm += "MERGE (t:Tweet {tid:{T1}}) SET t.quote_count={T2},t.reply_count={T3},t.datetime={T4},t.date={T5},t.like_count={T6},t.verified={T7},t.sentiment={T8},t.location={T9},t.retweet_count={T10},t.type={T11},t.tweet_text={T12},t.lang={T13}" 
stm += "MERGE (u)-[:POSTS]-(t)" 

stm3 = "MERGE (t:Tweet {tid:{T1}}) MERGE(mnt:User {screen_name:{MNT1}}) MERGE (t)-[:MENTIONS]-(mnt)"
stm4 = "MERGE (t:Tweet {tid:{T1}}) MERGE(l:Location {place:{T9}}) MERGE (t)-[:AT]-(l)"

f = open('dataset.json',"r")
print f.name
s1 = f.read()	
d = json.loads(s1)
tx = graph.begin()

for k in d.keys():
	# print k
	dt = {}
	dt['U1']=d[k]["author"]
	dt['U2']=d[k]["author_id"]
	dt['U3']=d[k]["author_screen_name"]
	dt['U4']=d[k]["author_profile_image"]
	dt['T1']=d[k]["tid"]
	dt['T2']=d[k]["quote_count"]
	dt['T3']=d[k]["reply_count"]
	dt['T4']=d[k]["datetime"]
	dt['T5']=d[k]["date"]
	dt['T6']=d[k]["like_count"]
	dt['T7']=d[k]["verified"]
	dt['T8']=d[k]["sentiment"]
	dt['T9']=d[k]["location"]
	dt['T10']=d[k]["retweet_count"]
	dt['T11']=d[k]["type"]
	dt['T12']=d[k]["tweet_text"]
	dt['T13']=d[k]["lang"]
	# dt['T14']=d[k]["media_list"]
	tx.append(stm,dt)
	if d[k]["mentions"] is not None:
		for h in d[k]["mentions"]:
			if h != '':
				dt['MNT1']=h
				tx.append(stm3,dt)
	if dt['T9'] is not None and dt['T9'] != "":
		tx.append(stm4,dt)
tx.commit()
f.close()				

print len(d.keys())
usr = raw_input("Enter author_screen_name")
ans=graph.run("match(u1:User)<-[:MENTIONS]-(t:Tweet)-[:MENTIONS]->(u2:User) where u1.screen_name={U} return u1.screen_name,u2.screen_name,collect(t.tid),count(*) order by count(*) DESC",{"U":usr}).data()
print dumps(ans)


htag = raw_input("Enter location ")
ans=graph.run("match(l:Location)<-[:AT]-(t:Tweet)-[:MENTIONS]->(u:User) where l.place={U} return l.place,u.screen_name,collect(t.tid),count(*) order by count(*) DESC",{"U":htag}).data()
print dumps(ans)
