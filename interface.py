from neo4j import GraphDatabase

class Interface:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()

    def bfs(self, start_node, last_node):
        # TODO: Implement this method
        with self._driver.session() as session:
            #session.run("CALL gds.graph.drop('breadthGraph')")
            session.run("""CALL gds.graph.project('breadthGraph', 'Location', 'TRIP')""")
            result = session.run("""
            MATCH (start:Location{name:$start_nodes}), (last:Location{name:$last_nodes})
            WITH id(start) AS source, id(last) AS targetNodes
            CALL gds.bfs.stream('breadthGraph', {
                sourceNode: source,
                targetNodes: targetNodes
                })
            YIELD path
            RETURN path
            """, start_nodes = start_node, last_nodes = last_node)
            
            session.run("CALL gds.graph.drop('breadthGraph')")
            
            return result.data()
            
            #return nodes
       
        #raise NotImplementedError

    def pagerank(self, max_iterations, weight_property):
        # TODO: Implement this method 
        with self._driver.session() as session:
            session.run("""
                CALL gds.graph.project(
                'pageGraph',
                'Location',
                'TRIP',
                {
                    relationshipProperties: 'distance'
                }
                )""")
            result = session.run("""
                CALL gds.pageRank.stream('pageGraph',{
                maxIterations: $max_iterations,
                dampingFactor: 0.85,
                relationshipWeightProperty: $weight_property
                })
                YIELD nodeId, score
                RETURN gds.util.asNode(nodeId).name AS name, score
                ORDER BY score DESC""",
                max_iterations=max_iterations,
                weight_property=weight_property
            )
         
            records = list(result)
            session.run("CALL gds.graph.drop('pageGraph')")
            return records[0], records[-1]
            
        #raise NotImplementedError

def main():
    # Initialize the interface
    interface = Interface(uri='bolt://localhost:7687', user='neo4j', password='project2phase1')

    # Run pagerank with max_iterations=20 and weight_property='distance'
    result1 = interface.pagerank(max_iterations=20, weight_property='distance')
    result2 = interface.bfs(159,212)
    
    # Print the results
    print(result1)
    print(result2)

    # Close the interface
    interface.close()

if __name__ == "__main__":
    main()