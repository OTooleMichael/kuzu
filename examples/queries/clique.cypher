MATCH (a:Person)-[e1:knows]->(b:Person)-[e2:knows]->(c:Person), (a)-[e3:knows]->(d:Person)-[e4:knows]->(c), (a)-[e5:knows]->(c), (b)-[e6:knows]->(d) RETURN COUNT(*)