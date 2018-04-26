from py2neo import Graph
from py2neo import Node
from py2neo import Relationship
from py2neo import authenticate
from flask import Flask,render_template,request


graph = Graph()
app = Flask(__name__)

@app.route("/")
def main():
    return render_template('index.html')

@app.route('/query1',methods=['POST','GET'])
def Q1():
    return render_template('q1.html')

@app.route('/process1',methods=['POST','GET'])
def process1():
	usr = request.form.get('expr')
	print usr
	ans=graph.run("match(u1:User)<-[:MENTIONS]-(t:Tweet)-[:MENTIONS]->(u2:User) where u1.screen_name={U} return u1.screen_name,u2.screen_name,collect(t.tid),count(*) order by count(*) DESC",{"U":usr}).data()
	ans1 = []
	for bla in ans:
		ans11=[]
		ans11.append(bla['u1.screen_name'])
		ans11.append(bla['u2.screen_name'])
		ans11.append(bla['collect(t.tid)'])
		ans11.append(bla['count(*)'])
		ans1.append(ans11)
	return render_template('q11.html',rows1=ans1)


@app.route('/query2')
def Q2():
	return render_template('q2.html')	

@app.route('/process2',methods=['POST','GET'])
def process2():
	htag = request.form.get('expr')
	print htag
	ans=graph.run("match(l:Location)<-[:AT]-(t:Tweet)-[:MENTIONS]->(u:User) where l.place={U} return l.place,u.screen_name,collect(t.tid),count(*) order by count(*) DESC",{"U":htag}).data()
	for bla in ans:
		print bla
	ans1 = []
	for bla in ans:
		ans11=[]
		ans11.append(bla['l.place'])
		ans11.append(bla['u.screen_name'])
		ans11.append(bla['collect(t.tid)'])
		ans11.append(bla['count(*)'])
		ans1.append(ans11)
	return render_template('q22.html',rows1=ans1)

if __name__ == "__main__":
    app.run()