/**
 * Universidad del Valle de Guatemala
 * Algoritmos y Estructuras de Datos
 * Hoja de Trabajo 10: implementacion de neo4j en java
 * Andrés Girón, Paulina Cano y Brandon Hernandez
 * 09 de novviembre del 2016
 */

import java.io.File;
import org.neo4j.cypher.internal.ExecutionEngine;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.logging.LogProvider;
import java.util.LinkedList;
import java.util.Iterator;

public class MAIN {
    
    //ENUMERACIÓN TIPO NODO, IMPLEMENTA LABEL
    public enum NodeType implements Label{
        People;
    }
    //ENUMERACIÓN TIPO RELACIÓN, IMPLEMENTA RELATIONSHIP-TYPE
    public enum RelationType implements RelationshipType{
        Email;
    }

    static Result res1, res2, res3, res4, res5, res6;

	public static void main(String[] args) {

        //FACTORY TO CREATE A DATABASE
        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();

        //ADDRESS OF THE DATABASE, IT DEPENDS ON EACH COMPUTER
        File address = new File("C:\\Users\\usuario\\Documents\\Neo4j\\default.graphdb");

        //FILE OBJECT TO REACH THE GRAPH
        GraphDatabaseService graphDb = dbFactory.newEmbeddedDatabase(address);
        graphDb.execute("MATCH (n)\n" + "OPTIONAL MATCH (n)-[r]-()\n" + "DELETE n,r");

        //TRNSACTION WITH NEO4J

        try (Transaction tx = graphDb.beginTx()){
            
            //PEOPLE NODES FOR EACH WORKER
            //ID FOR THE WORKER
            //& NAME OF THE WORKER

            Node Per1 = graphDb.createNode(NodeType.People);
            Per1.setProperty("ID",1);
            Per1.setProperty("Nombre", "Persona 1");
            
            Node Per2 = graphDb.createNode(NodeType.People);
            Per2.setProperty("ID",2);
            Per2.setProperty("Nombre", "Persona 2");
            
            Node Per3 = graphDb.createNode(NodeType.People);
            Per3.setProperty("ID",3);
            Per3.setProperty("Nombre", "Persona 3");
            
            Node Per4 = graphDb.createNode(NodeType.People);
            Per4.setProperty("ID",4);
            Per4.setProperty("Nombre", "Persona 4");
            
            Node Per5 = graphDb.createNode(NodeType.People);
            Per5.setProperty("ID",5);
            Per5.setProperty("Nombre", "Persona 5");
            
            Node Per6 = graphDb.createNode(NodeType.People);
            Per6.setProperty("ID",6);
            Per6.setProperty("Nombre", "Persona 6");
            
            Node Per7 = graphDb.createNode(NodeType.People);
            Per7.setProperty("ID",7);
            Per7.setProperty("Nombre", "Persona 7");
            
            Node Per8 = graphDb.createNode(NodeType.People);
            Per8.setProperty("ID",8);
            Per8.setProperty("Nombre", "Persona 8");
            
            Node Per9 = graphDb.createNode(NodeType.People);
            Per9.setProperty("ID",9);
            Per9.setProperty("Nombre", "Persona 9");
            
            Node Per10 = graphDb.createNode(NodeType.People);
            Per10.setProperty("ID",10);
            Per10.setProperty("Nombre", "Persona 10");
            
            Node Per11 = graphDb.createNode(NodeType.People);
            Per11.setProperty("ID",11);
            Per11.setProperty("Nombre", "Persona 11");
            
            Node Per12 = graphDb.createNode(NodeType.People);
            Per12.setProperty("ID",12);
            Per12.setProperty("Nombre", "Persona 12");
            
            Node Per13 = graphDb.createNode(NodeType.People);
            Per13.setProperty("ID",13);
            Per13.setProperty("Nombre", "Persona 13");
            
            Node Per14 = graphDb.createNode(NodeType.People);
            Per14.setProperty("ID",14);
            Per14.setProperty("Nombre", "Persona 14");
            
            tx.success();        
        } 
        graphDb.shutdown();
    }
}
