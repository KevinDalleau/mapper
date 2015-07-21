package fr.kevindalleau.Mapper;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;

import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;


public class Mapper implements Serializable {

	private static final long serialVersionUID = 1234;
	private HashMap<String, String> umlsid_to_pharmgkb;
	
	public Mapper() {
		this.umlsid_to_pharmgkb = new HashMap<String,String>();
		File file_umls_to_pharmgkb = new File("./umlsid_to_pharmgkb.ser");
		if(file_umls_to_pharmgkb.exists()) {
			try {
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file_umls_to_pharmgkb));
				try {
					this.umlsid_to_pharmgkb = (HashMap<String, String>) ois.readObject();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			 this.umlsid_to_pharmgkb = getUmlsToPharmGKB();
			try {
				ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file_umls_to_pharmgkb));
				oos.writeObject(umlsid_to_pharmgkb);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		

	}
	
	public String getPharmGKB_from_UMLS(String umls) {
		return this.getUmlsid_to_pharmgkb().get(umls);
	}
	
	private static HashMap<String,String> getUmlsToPharmGKB() {
		HashMap<String,String> umlsToPharmGKBMappings = new HashMap<String, String>();
		String query = "SELECT ?umls_id ?pharmgkb_id\n" + 
				"WHERE {\n" + 
				"?umls_id_uri <http://biodb.jp/mappings/to_pharmgkb_id> ?pharmgkb_id_uri. \n" + 
				"FILTER regex(str(?umls_id_uri), \"^http://biodb.jp/mappings/umls_id/\")\n" + 
				"  BIND(REPLACE(str(?umls_id_uri), \"http://biodb.jp/mappings/umls_id/\",\"\") AS ?umls_id)\n" + 
				"  BIND(REPLACE(str(?pharmgkb_id_uri), \"http://biodb.jp/mappings/pharmgkb_id/\",\"\") AS ?pharmgkb_id)\n" + 
				"}\n" + 
				"";
		
		QueryEngineHTTP queryExec = (QueryEngineHTTP) QueryExecutionFactory.sparqlService("http://cassandra.kevindalleau.fr/mappings/sparql", query);
		queryExec.addParam("timeout","3600000");
		ResultSet results = queryExec.execSelect();
		
		while(results.hasNext()) {
			QuerySolution solution = results.nextSolution();
			RDFNode umlsNode = solution.get("umls_id");
			RDFNode pharmGKBNode = solution.get("pharmgkb_id");
			umlsToPharmGKBMappings.put(umlsNode.toString(), pharmGKBNode.toString());	
		}
		return umlsToPharmGKBMappings;
	}

	private HashMap<String, String> getUmlsid_to_pharmgkb() {
		return umlsid_to_pharmgkb;
	}

	private void setUmlsid_to_pharmgkb(HashMap<String, String> umlsid_to_pharmgkb) {
		this.umlsid_to_pharmgkb = umlsid_to_pharmgkb;
	}
	

}
