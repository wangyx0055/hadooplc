//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.1-b02-fcs 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2009.02.09 at 02:11:12 PM EST 
//


package edu.whu.cs.hadoopdb.catalog.xml;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
/**
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Node", propOrder = {
    "relations"
})

public class Node {
    @XmlElement(name = "Relations", required = true)
    protected List<Relation> relations;
    @XmlAttribute(name = "Location")
    protected String location;
    @XmlAttribute(name = "Path")
    protected String path;
    @XmlAttribute(name = "Analyzer")
    protected String analyzer;
    @XmlAttribute(name = "Version")
    protected String version;
    public List<Relation> getRelations() {
        if (relations == null) {
            relations = new ArrayList<Relation>();
        }
        return this.relations;
    }
    
    public String getLocation() {
        return location;
    }

    public void setLocation(String value) {
        this.location = value;
    }
    public String getPath() {
    	return path;
    }
    
    public void setPath(String value) {
    	this.path = value;
    }
    
    public String getAnalyzer() {
    	return analyzer;
    }
    
    public void setAnalyzer(String value) {
    	this.analyzer = value;
    }
    
    public String getVersion() {
    	return version;
    }
    
    public void setVersion(String value) {
    	this.version = value;
    }
}


