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
	private HashMap<String, String> pharmgkb_to_umlsid;
	private HashMap<String, String> umlsid_to_pharmgkb;
	private HashMap<String, ArrayList<String>> pharmgkb_to_stitch;
	private HashMap<String, String> pharmgkb_to_drugbank;
	private HashMap<String, String> pharmgkb_to_uniprot;
	
	public String getUMLS_from_PharmGKB(String pharmgkb) {
		return this.getPharmgkb_to_umlsid().get(pharmgkb);
	}
	public String getPharmGKB_from_UMLS(String umls) {
		return this.getUmlsid_to_pharmgkb().get(umls);
	}
	
	public String getDrugbank_from_PharmGKB(String pharmgkb) {
		return this.getPharmGKBToDrugBank().get(pharmgkb);
	}
	
	public String getUniProt_from_PharmGKB(String uniprot) {
		return this.getPharmGKBToUniProt().get(uniprot);
	}
	
	public ArrayList<String> getStitch_from_PharmGKB(String pharmgkb) {
		return this.getPharmgkb_to_stitch(pharmgkb);
	}
	
	public Mapper() {
		this.pharmgkb_to_umlsid = new HashMap<String, String>();
		this.umlsid_to_pharmgkb = new HashMap<String,String>();
		this.pharmgkb_to_stitch = new HashMap<String, ArrayList<String>>();
				
		File file_pharmgkb_to_umls = new File("./pharmgkb_to_umlsid.ser");
		File file_umls_to_pharmgkb = new File("./umlsid_to_pharmgkb.ser");
		File file_pharmgkb_to_stitch = new File("./pharmgkb_to_stitch.ser");
		
		if(file_pharmgkb_to_umls.exists()) {
			
			try {
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file_pharmgkb_to_umls));
				try {
					this.pharmgkb_to_umlsid = (HashMap<String, String>) ois.readObject();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		else {
			this.pharmgkb_to_umlsid = getPharmGKBToUMLS();
			try {
				ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file_pharmgkb_to_umls));
				oos.writeObject(pharmgkb_to_umlsid);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
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
	
	private HashMap<String,String> getPharmGKBToDrugBank() {
		HashMap<String,String> PharmGKBtoDrugBankMappings = new HashMap<String, String>();
		String query = "SELECT ?pharmgkb_id ?drugbank_id\n" + 
				"				WHERE { \n" + 
				"				?pharmgkb_id_uri <http://biodb.jp/mappings/to_drugbank_id> ?drugbank_id_uri\n" + 
				"				FILTER regex(str(?drugbank_id_uri), \"http://biodb.jp/mappings/drugbank_id/\") \n" + 
				"  				FILTER regex(str(?pharmgkb_id_uri), \"http://biodb.jp/mappings/pharmgkb_id/\") \n" + 
				"				BIND(REPLACE(str(?drugbank_id_uri), \"http://biodb.jp/mappings/drugbank_id/\",\"\") AS ?drugbank_id) \n" + 
				"				BIND(REPLACE(str(?pharmgkb_id_uri), \"http://biodb.jp/mappings/pharmgkb_id/\",\"\") AS ?pharmgkb_id) \n" + 
				"								}";
		
		QueryEngineHTTP queryExec = (QueryEngineHTTP) QueryExecutionFactory.sparqlService("http://cassandra.kevindalleau.fr/mappings/sparql", query);
		queryExec.addParam("timeout","3600000");
		ResultSet results = queryExec.execSelect();
		
		while(results.hasNext()) {
			QuerySolution solution = results.nextSolution();
			RDFNode drugbankNode = solution.get("drugbank_id");
			RDFNode pharmGKBNode = solution.get("pharmgkb_id");
			PharmGKBtoDrugBankMappings.put(pharmGKBNode.toString(),drugbankNode.toString());	
		}
		return PharmGKBtoDrugBankMappings;
	}
	
	private HashMap<String,String> getPharmGKBToUniProt() {
		HashMap<String,String> PharmGKBToUniProtMappings = new HashMap<String, String>();
		String query = "PREFIX http: <http://www.w3.org/2011/http#>\n" + 
				"prefix owl: <http://www.w3.org/2002/07/owl#>\n" + 
				"prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
				"SELECT DISTINCT ?pharmgkb_id ?uniprot_id\n" + 
				"WHERE {\n" + 
				"?pharmgkb_uri <http://biodb.jp/mappings/to_uniprot_id> ?uniprot_uri.\n" + 
				"BIND(REPLACE(str(?pharmgkb_uri), \"http://biodb.jp/mappings/pharmgkb_id/\",\"\") AS ?pharmgkb_id)\n" + 
				"BIND(REPLACE(str(?uniprot_uri), \"http://biodb.jp/mappings/uniprot_id/\",\"\") AS ?uniprot_id)\n" + 
				"}";
		
		QueryEngineHTTP queryExec = (QueryEngineHTTP) QueryExecutionFactory.sparqlService("http://cassandra.kevindalleau.fr/mappings/sparql", query);
		queryExec.addParam("timeout","3600000");
		ResultSet results = queryExec.execSelect();
		
		while(results.hasNext()) {
			QuerySolution solution = results.nextSolution();
			RDFNode uniProtNode = solution.get("uniprot_id");
			RDFNode pharmGKBNode = solution.get("pharmgkb_id");
			PharmGKBToUniProtMappings.put(pharmGKBNode.toString(),uniProtNode.toString());	
		}
		return PharmGKBToUniProtMappings;
	}
	
	private static HashMap<String,String> getPharmGKBToUMLS() {
		HashMap<String,String> PharmGKBToUmlsMappings = new HashMap<String, String>();
		String query = "SELECT ?umls_id ?pharmgkb_id\n" + 
				"				WHERE {\n" + 
				"				?pharmgkb_id_uri <http://biodb.jp/mappings/to_umls_id> ?umls_id_uri. \n" + 
				"				FILTER regex(str(?umls_id_uri), \"^http://biodb.jp/mappings/umls_id/\")\n" + 
				"				  BIND(REPLACE(str(?umls_id_uri), \"http://biodb.jp/mappings/umls_id/\",\"\") AS ?umls_id)\n" + 
				"				  BIND(REPLACE(str(?pharmgkb_id_uri), \"http://biodb.jp/mappings/pharmgkb_id/\",\"\") AS ?pharmgkb_id)\n" + 
				"				}";
		
		QueryEngineHTTP queryExec = (QueryEngineHTTP) QueryExecutionFactory.sparqlService("http://cassandra.kevindalleau.fr/mappings/sparql", query);
		queryExec.addParam("timeout","3600000");
		ResultSet results = queryExec.execSelect();
		
		while(results.hasNext()) {
			QuerySolution solution = results.nextSolution();
			RDFNode umlsNode = solution.get("umls_id");
			RDFNode pharmGKBNode = solution.get("pharmgkb_id");
			PharmGKBToUmlsMappings.put(pharmGKBNode.toString(),umlsNode.toString());	
		}
		return PharmGKBToUmlsMappings;
	}

	public HashMap<String, String> getUmlsid_to_pharmgkb() {
		return umlsid_to_pharmgkb;
	}

	private void setUmlsid_to_pharmgkb(HashMap<String, String> umlsid_to_pharmgkb) {
		this.umlsid_to_pharmgkb = umlsid_to_pharmgkb;
	}

	private ArrayList<String> getPharmgkb_to_stitch(String pharmgkb) {
		return pharmgkb_to_stitch.get(pharmgkb);
	}
	public HashMap<String, String> getPharmgkb_to_umlsid() {
		return pharmgkb_to_umlsid;
	}
	public void setPharmgkb_to_umlsid(HashMap<String, String> pharmgkb_to_umlsid) {
		this.pharmgkb_to_umlsid = pharmgkb_to_umlsid;
	}
	public HashMap<String, ArrayList<String>> getPharmgkb_to_stitch() {
		return pharmgkb_to_stitch;
	}
	public void setPharmgkb_to_stitch(HashMap<String, ArrayList<String>> pharmgkb_to_stitch) {
		this.pharmgkb_to_stitch = pharmgkb_to_stitch;
	}
	public HashMap<String, String> getPharmgkb_to_drugbank() {
		return pharmgkb_to_drugbank;
	}
	public void setPharmgkb_to_drugbank(HashMap<String, String> pharmgkb_to_drugbank) {
		this.pharmgkb_to_drugbank = pharmgkb_to_drugbank;
	}
	public HashMap<String, String> getPharmgkb_to_uniprot() {
		return pharmgkb_to_uniprot;
	}
	public void setPharmgkb_to_uniprot(HashMap<String, String> pharmgkb_to_uniprot) {
		this.pharmgkb_to_uniprot = pharmgkb_to_uniprot;
	}

	

}
