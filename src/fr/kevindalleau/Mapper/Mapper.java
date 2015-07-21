package fr.kevindalleau.Mapper;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;


public class Mapper implements Serializable {

	private static final long serialVersionUID = 1234;
	private HashMap<String, String> umlsid_to_pharmgkb;
	private HashMap<String, ArrayList<String>> pharmgkb_to_stitch;
	
	public String getPharmGKB_from_UMLS(String umls) {
		return this.getUmlsid_to_pharmgkb().get(umls);
	}
	
	public ArrayList<String> getStitch_from_PharmGKB(String pharmgkb) {
		return this.getPharmgkb_to_stitch(pharmgkb);
	}
	
	public Mapper() {
		this.umlsid_to_pharmgkb = new HashMap<String,String>();
		this.pharmgkb_to_stitch = new HashMap<String, ArrayList<String>>();
		
		File file_umls_to_pharmgkb = new File("./umlsid_to_pharmgkb.ser");
		File file_pharmgkb_to_stitch = new File("./pharmgkb_to_stitch.ser");
		
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
		
		if(file_pharmgkb_to_stitch.exists()) {
			
			try {
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file_pharmgkb_to_stitch));
				try {
					this.pharmgkb_to_stitch = (HashMap<String, ArrayList<String>>) ois.readObject();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		else {
			this.pharmgkb_to_stitch = getPharmgkbToStitch();
			try {
				ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file_pharmgkb_to_stitch));
				oos.writeObject(pharmgkb_to_stitch);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		

	}
	
	
	private static HashMap<String,ArrayList<String>> getPharmgkbToStitch() {
		HashMap<String,ArrayList<String>> umlsToPharmGKBMappings = new HashMap<String, ArrayList<String>>();
		String queryLinks = "SELECT DISTINCT ?drug ?stitch_id\n" + 
				"		WHERE {\n" + 
				"		?drug_uri <http://biodb.jp/mappings/to_c_id> ?stitch_id_uri.\n" + 
				"		FILTER regex(str(?drug_uri), \"^http://biodb.jp/mappings/pharmgkb_id/\")\n" + 
				"		BIND(REPLACE(str(?drug_uri), \"^http://biodb.jp/mappings/pharmgkb_id/\",\"\") AS ?drug)\n" + 
				"		BIND(REPLACE(str(?stitch_id_uri), \"^http://biodb.jp/mappings/c_id/\",\"\") AS ?stitch_id)\n" + 
				"		}\n" + 
				"";
		
		Query query = QueryFactory.create(queryLinks);
		QueryExecution queryExec = QueryExecutionFactory.sparqlService("http://cassandra.kevindalleau.fr/mappings/sparql", queryLinks);
		ResultSet results = queryExec.execSelect();
		while(results.hasNext()) {
			QuerySolution solution = results.nextSolution();
			RDFNode drugNode = solution.get("drug");
			RDFNode stitchIdNode = solution.get("stitch_id");
			if(umlsToPharmGKBMappings.get(drugNode.toString()) != null) {
				umlsToPharmGKBMappings.get(drugNode.toString()).add(stitchIdNode.toString());
			}
			else {
				ArrayList<String> stitchIds = new ArrayList<String>();
				stitchIds.add(stitchIdNode.toString());
				umlsToPharmGKBMappings.put(drugNode.toString(), stitchIds);
			}
		};
		queryExec.close();
		return umlsToPharmGKBMappings;
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

	private ArrayList<String> getPharmgkb_to_stitch(String pharmgkb) {
		return pharmgkb_to_stitch.get(pharmgkb);
	}

	

}
