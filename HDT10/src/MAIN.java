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
    
    static Result res1, res2, res3, res4, res5, res6;
    
    //ENUMERACIÓN TIPO NODO, IMPLEMENTA LABEL
    public enum NodeType implements Label{
        People;
    }
    //ENUMERACIÓN TIPO RELACIÓN, IMPLEMENTA RELATIONSHIP-TYPE
    public enum RelationType implements RelationshipType{
        Email;
    }

	public static void main(String[] args) {

        //FACTORY TO CREATE A DATABASE
        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();

        //ADDRESS OF THE DATABASE, IT DEPENDS ON EACH COMPUTER
        File address = new File("C:\\Users\\usuario\\Documents\\Neo4j\\default.graphdb");

        //FILE OBJECT TO REACH THE GRAPH
        GraphDatabaseService DataBase = dbFactory.newEmbeddedDatabase(address);
        DataBase.execute("MATCH (n)\n" + "OPTIONAL MATCH (n)-[r]-()\n" + "DELETE n,r");

        //TRNSACTION WITH NEO4J
	try (Transaction tx = DataBase.beginTx()){
            
//**************** P E O P L E * N O D E S * F O R * E A C H * W O R K E R *******************************************************************
            
            //ID FOR THE WORKER
            //& NAME OF THE WORKER

            Node Per1 = DataBase.createNode(NodeType.People);
            Per1.setProperty("ID",1);
            Per1.setProperty("Nombre", "Persona 1");
            
            Node Per2 = DataBase.createNode(NodeType.People);
            Per2.setProperty("ID",2);
            Per2.setProperty("Nombre", "Persona 2");
            
            Node Per3 = DataBase.createNode(NodeType.People);
            Per3.setProperty("ID",3);
            Per3.setProperty("Nombre", "Persona 3");
            
            Node Per4 = DataBase.createNode(NodeType.People);
            Per4.setProperty("ID",4);
            Per4.setProperty("Nombre", "Persona 4");
            
            Node Per5 = DataBase.createNode(NodeType.People);
            Per5.setProperty("ID",5);
            Per5.setProperty("Nombre", "Persona 5");
            
            Node Per6 = DataBase.createNode(NodeType.People);
            Per6.setProperty("ID",6);
            Per6.setProperty("Nombre", "Persona 6");
            
            Node Per7 = DataBase.createNode(NodeType.People);
            Per7.setProperty("ID",7);
            Per7.setProperty("Nombre", "Persona 7");
           
            Node Per8 = DataBase.createNode(NodeType.People);
            Per8.setProperty("ID",8);
            Per8.setProperty("Nombre", "Persona 8");
            
            Node Per9 = DataBase.createNode(NodeType.People);
            Per9.setProperty("ID",9);
            Per9.setProperty("Nombre", "Persona 9");
            
            Node Per10 = DataBase.createNode(NodeType.People);
            Per10.setProperty("ID",10);
            Per10.setProperty("Nombre", "Persona 10");
            
            Node Per11 = DataBase.createNode(NodeType.People);
            Per11.setProperty("ID",11);
            Per11.setProperty("Nombre", "Persona 11");
            
            Node Per12 = DataBase.createNode(NodeType.People);
            Per12.setProperty("ID",12);
            Per12.setProperty("Nombre", "Persona 12");
            
            Node Per13 = DataBase.createNode(NodeType.People);
            Per13.setProperty("ID",13);
            Per13.setProperty("Nombre", "Persona 13");
            
            Node Per14 = DataBase.createNode(NodeType.People);
            Per14.setProperty("ID",14);
            Per14.setProperty("Nombre", "Persona 14");
			
//********************* R E L A C I O N E S *********************************************************************
	
	    //Se crean relaciones entre los empleados, las cuales indican que se han enviado correos
            //Se define cuantos correos ha mandado el empleado al otro
			
            Relationship P1P2 = Per1.createRelationshipTo(Per2,RelationType.Email);
            P1P2.setProperty("Cantidad",3);
            
            Relationship P1P3 = Per1.createRelationshipTo(Per3,RelationType.Email);
            P1P3.setProperty("Cantidad",2);
            
            Relationship P1P9 = Per1.createRelationshipTo(Per9,RelationType.Email);
            P1P9.setProperty("Cantidad",6);
            
            Relationship P2P1 = Per2.createRelationshipTo(Per1,RelationType.Email);
            P2P1.setProperty("Cantidad",5);
            
            Relationship P2P3 = Per2.createRelationshipTo(Per3,RelationType.Email);
            P2P3.setProperty("Cantidad",8);
            
            Relationship P2P4 = Per2.createRelationshipTo(Per4,RelationType.Email);
            P2P4.setProperty("Cantidad",2);
            
            Relationship P2P12 = Per2.createRelationshipTo(Per12,RelationType.Email);
            P2P12.setProperty("Cantidad",1);
            
            Relationship P3P1 = Per3.createRelationshipTo(Per1,RelationType.Email);
            P3P1.setProperty("Cantidad",5);
            
            Relationship P3P4 = Per3.createRelationshipTo(Per4,RelationType.Email);
            P3P4.setProperty("Cantidad",2);
            
            Relationship P4P5 = Per4.createRelationshipTo(Per5,RelationType.Email);
            P4P5.setProperty("Cantidad",5);
            
            Relationship P4P11 = Per4.createRelationshipTo(Per11,RelationType.Email);
            P4P11.setProperty("Cantidad",2);
            
            Relationship P4P13 = Per4.createRelationshipTo(Per13,RelationType.Email);
            P4P13.setProperty("Cantidad",3);
            
            Relationship P4P14 = Per4.createRelationshipTo(Per14,RelationType.Email);
            P4P14.setProperty("Cantidad",7);
            
           Relationship P5P4 = Per5.createRelationshipTo(Per4,RelationType.Email);
            P5P4.setProperty("Cantidad",2);
            
            Relationship P5P6 = Per5.createRelationshipTo(Per6,RelationType.Email);
            P5P6.setProperty("Cantidad",6);
            
            Relationship P5P11 = Per5.createRelationshipTo(Per11,RelationType.Email);
            P5P11.setProperty("Cantidad",4);
            
            Relationship P5P12 = Per5.createRelationshipTo(Per12,RelationType.Email);
           P5P12.setProperty("Cantidad",3);
            
            Relationship P5P13 = Per5.createRelationshipTo(Per13,RelationType.Email);
            P5P13.setProperty("Cantidad",7);
            
            Relationship P5P14 = Per5.createRelationshipTo(Per14,RelationType.Email);
            P5P14.setProperty("Cantidad",9);
       
            Relationship P6P4 = Per6.createRelationshipTo(Per4,RelationType.Email);
            P6P4.setProperty("Cantidad",6);
            
            Relationship P6P5 = Per6.createRelationshipTo(Per5,RelationType.Email);
            P6P5.setProperty("Cantidad",2);
            
            Relationship P6P12 = Per6.createRelationshipTo(Per12,RelationType.Email);
            P6P12.setProperty("Cantidad",7);
            
            Relationship P6P13 = Per6.createRelationshipTo(Per13,RelationType.Email);
            P6P13.setProperty("Cantidad",7);
            
            Relationship P7P8 = Per7.createRelationshipTo(Per8,RelationType.Email);
            P7P8.setProperty("Cantidad",12);
            
            Relationship P7P9 = Per7.createRelationshipTo(Per9,RelationType.Email);
            P7P9.setProperty("Cantidad",13);
            
            Relationship P7P11 = Per7.createRelationshipTo(Per11,RelationType.Email);
            P7P11.setProperty("Cantidad",1);
            
            Relationship P8P6 = Per8.createRelationshipTo(Per6,RelationType.Email);
            P8P6.setProperty("Cantidad",3);
            
            Relationship P8P7 = Per8.createRelationshipTo(Per7,RelationType.Email);
            P8P7.setProperty("Cantidad",14);
            
            Relationship P8P9 = Per8.createRelationshipTo(Per9,RelationType.Email);
            P8P9.setProperty("Cantidad",21);
            
            Relationship P8P10 = Per8.createRelationshipTo(Per10,RelationType.Email);
            P8P10.setProperty("Cantidad",2);
            
            Relationship P9P5 = Per9.createRelationshipTo(Per5,RelationType.Email);
            P9P5.setProperty("Cantidad",4);
            
            Relationship P9P7 = Per9.createRelationshipTo(Per7,RelationType.Email);
            P9P7.setProperty("Cantidad",12);
            
            Relationship P9P8 = Per9.createRelationshipTo(Per8,RelationType.Email);
            P9P8.setProperty("Cantidad",23);
            
            Relationship P10P4 = Per10.createRelationshipTo(Per4,RelationType.Email);
            P10P4.setProperty("Cantidad",9);
            
            Relationship P10P5 = Per10.createRelationshipTo(Per5,RelationType.Email);
            P10P5.setProperty("Cantidad",7);
            
            Relationship P10P6 = Per10.createRelationshipTo(Per6,RelationType.Email);
            P10P6.setProperty("Cantidad",1);
            
            Relationship P10P7 = Per10.createRelationshipTo(Per7,RelationType.Email);
            P10P7.setProperty("Cantidad",1);
            
            Relationship P10P11 = Per10.createRelationshipTo(Per11,RelationType.Email);
            P10P11.setProperty("Cantidad",5);
            
            Relationship P10P12 = Per10.createRelationshipTo(Per12,RelationType.Email);
            P10P12.setProperty("Cantidad",4);
            
            Relationship P10P13 = Per10.createRelationshipTo(Per13,RelationType.Email);
            P10P13.setProperty("Cantidad",8);
            
            Relationship P10P14 = Per10.createRelationshipTo(Per14,RelationType.Email);
            P10P14.setProperty("Cantidad",7);
            
            Relationship P11P4 = Per11.createRelationshipTo(Per4,RelationType.Email);
            P11P4.setProperty("Cantidad",4);
            
            Relationship P11P6 = Per11.createRelationshipTo(Per6,RelationType.Email);
            P11P6.setProperty("Cantidad",1);
            
            Relationship P11P10 = Per11.createRelationshipTo(Per10,RelationType.Email);
            P11P10.setProperty("Cantidad",1);
                      
            Relationship P11P13 = Per11.createRelationshipTo(Per13,RelationType.Email);
            P11P13.setProperty("Cantidad",9);
            
            Relationship P11P14 = Per11.createRelationshipTo(Per14,RelationType.Email);
            P11P14.setProperty("Cantidad",1);
            
            Relationship P12P4 = Per12.createRelationshipTo(Per4,RelationType.Email);
            P12P4.setProperty("Cantidad",4);
            
            Relationship P12P5 = Per12.createRelationshipTo(Per5,RelationType.Email);
            P12P5.setProperty("Cantidad",4);
            
            Relationship P12P6 = Per12.createRelationshipTo(Per6,RelationType.Email);
            P12P6.setProperty("Cantidad",9);
            
            Relationship P12P10 = Per12.createRelationshipTo(Per10,RelationType.Email);
            P12P10.setProperty("Cantidad",2);
            
            Relationship P12P11 = Per12.createRelationshipTo(Per11,RelationType.Email);
            P12P11.setProperty("Cantidad",4);
            
            Relationship P12P13 = Per12.createRelationshipTo(Per13,RelationType.Email);
            P12P13.setProperty("Cantidad",8);
            
            Relationship P12P14 = Per12.createRelationshipTo(Per14,RelationType.Email);
            P12P14.setProperty("Cantidad",9);
            
            Relationship P13P4 = Per13.createRelationshipTo(Per4,RelationType.Email);
            P13P4.setProperty("Cantidad",1);
            
            Relationship P13P5 = Per13.createRelationshipTo(Per5,RelationType.Email);
            P13P5.setProperty("Cantidad",3);
            
            Relationship P13P10 = Per13.createRelationshipTo(Per10,RelationType.Email);
            P13P10.setProperty("Cantidad",2);
            
            Relationship P13P11= Per13.createRelationshipTo(Per11,RelationType.Email);
            P13P11.setProperty("Cantidad",3);
            
            Relationship P13P12 = Per13.createRelationshipTo(Per2,RelationType.Email);
            P13P12.setProperty("Cantidad",2);
            
            Relationship P14P4 = Per14.createRelationshipTo(Per4,RelationType.Email);
            P14P4.setProperty("Cantidad",7);
            
            Relationship P14P10 = Per14.createRelationshipTo(Per10,RelationType.Email);
            P14P10.setProperty("Cantidad",6);
            
            Relationship P14P11 = Per14.createRelationshipTo(Per11,RelationType.Email);
            P14P11.setProperty("Cantidad",8);
            
            Relationship P14P12 = Per14.createRelationshipTo(Per12,RelationType.Email);
            P14P12.setProperty("Cantidad",3);
            
//************* M O S T R A R * R E L A C I O N E S * E N * P A N T A L L A ****************************************************

            System.out.println("Se imprimen las relaciones del grafo:\n");
            
            //QUERIES PARA VER NODOS Y RELACIONES
            res1 = DataBase.execute("MATCH (P1:People)-[C:Email]->(P2:People) RETURN P1.Nombre");
            res2 = DataBase.execute("MATCH (P1:People)-[C:Email]->(P2:People) RETURN P2.Nombre");
            res3 = DataBase.execute("MATCH (P1:People)-[C:Email]->(P2:People) RETURN C.Cantidad");
            
            //RESULT PUEDE RETORNAR UN ITERADOR
            Iterator<String>r1=res1.columnAs("P1.Nombre");
            Iterator<String>r2=res2.columnAs("P2.Nombre");
            Iterator<String>r3=res3.columnAs("C.Cantidad");
            
            //LINKED LISTS PARA INGRESAR LOS DATOS
            LinkedList<String> resultado1 = new LinkedList();
            LinkedList<String> resultado2 = new LinkedList();
            LinkedList<Object> resultado3 = new LinkedList();
            
            //ADICION DE LOS DATOS A LA LISTA
            while( r1.hasNext() & r2.hasNext() & r3.hasNext()){
                resultado1.add(r1.next());
                resultado2.add(r2.next());
                resultado3.add(r3.next());
            }        
            
            //TAMAÑO DE LA LISTA
            int size1 = resultado1.size();
            
            //IMPRESIÓN DE DATOS
            for(int i=0; i<size1; i++){
                System.out.println("La "+resultado1.get(i)+" envió "+resultado3.get(i)+" correos a la "+resultado2.get(i));
            }

//********* I N C I S O * A ******************************************************************************************
//********* SE MUESTRAN LAS RELACIONES EN DONDE SE HAYAN MANDADO MAS DE 6 CORREOS ***********************************
	
	    //EL PROCEDIMIENTO ES IGUAL, PERO AHORA LIMITADO A 6
            System.out.println("\n\n INCISO A ");
	    System.out.println("Las siguientes personas han enviado 6 correos o más:\n");
		
            res4 = DataBase.execute("MATCH (P1:People)-[C:Email]->(P2:People) WHERE C.Cantidad > 6 RETURN P1.Nombre");
            res5 = DataBase.execute("MATCH (P1:People)-[C:Email]->(P2:People) WHERE C.Cantidad > 6 RETURN P2.Nombre");
            res6 = DataBase.execute("MATCH (P1:People)-[C:Email]->(P2:People) WHERE C.Cantidad > 6 RETURN C.Cantidad");
            
            Iterator<String>r4=res4.columnAs("P1.Nombre");
            Iterator<String>r5=res5.columnAs("P2.Nombre");
            Iterator<String>r6=res6.columnAs("C.Cantidad");
            
            LinkedList<String> resultado4 = new LinkedList();
            LinkedList<String> resultado5 = new LinkedList();
            LinkedList<Object> resultado6 = new LinkedList();
            
            //ADICION DE LOS DATOS A LA LISTA
            while( r4.hasNext() & r5.hasNext() & r6.hasNext()){
                resultado4.add(r4.next());
                resultado5.add(r5.next());
                resultado6.add(r6.next());
            }  
            
            int size2 = resultado4.size();
            
	    //IMPRESION DE LOS RESULTADOS
            for(int i=0; i<size2; i++){
                System.out.println("La "+resultado4.get(i)+" envió "+resultado6.get(i)+" correos a la "+resultado5.get(i));
            }
            System.out.println("");
            
            tx.success();        
        }
        //ESTO APAGA LA BASE DE DATOS, TODO SE DA POR TERMIANDO
        DataBase.shutdown();
    }
}
