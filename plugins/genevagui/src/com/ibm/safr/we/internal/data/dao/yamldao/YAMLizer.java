package com.ibm.safr.we.internal.data.dao.yamldao;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.flogger.FluentLogger;
import com.ibm.safr.we.constants.ComponentType;
import com.ibm.safr.we.data.transfer.SAFRTransfer;
import com.ibm.safr.we.model.CodeSet;
import com.ibm.safr.we.data.transfer.ControlRecordTransfer;
import com.ibm.safr.we.data.transfer.PhysicalFileTransfer;
import com.ibm.safr.we.data.transfer.UserExitRoutineTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLogicalFileTransfer;

public class YAMLizer {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());;

    public static SAFRTransfer readYaml(Path input, ComponentType type) {
    	SAFRTransfer txfr = null;
        yamlMapper.findAndRegisterModules();
        try {
        	switch(type) {
        	case ControlRecord:
        		return  yamlMapper.readValue(input.toFile(), ControlRecordTransfer.class);
        	case UserExitRoutine:
        		return  yamlMapper.readValue(input.toFile(), UserExitRoutineTransfer.class);
        	case PhysicalFile:
        		return yamlMapper.readValue(input.toFile(), PhysicalFileTransfer.class);
        	case LogicalFile:
        		return yamlMapper.readValue(input.toFile(), YAMLLogicalFileTransfer.class);
        	}
        } catch (IOException e) {
            logger.atSevere().log("read coverage failed\n%s", e.getMessage());
        };
        return txfr;
    }

    
    public static void writeYaml(Path output, SAFRTransfer txfr) {
        try {
            yamlMapper.writeValue(output.toFile(), txfr);
        } catch (IOException e) {
            logger.atSevere().log("write coverage failed\n%s", e.getMessage());
        }
    }
    public static void writeYamlCodes(Path output, Map<String, CodeSet> codemap) {
        try {
            yamlMapper.writeValue(output.toFile(), codemap);
        } catch (IOException e) {
            logger.atSevere().log("write coverage failed\n%s", e.getMessage());
        }
    }
    public static Map<String, CodeSet> readYamlCodes(Path input) {
    	Map<String, CodeSet> codeSet = new TreeMap<>();
        try {
        	TypeReference<TreeMap<String, CodeSet>> typeRef = new TypeReference<TreeMap<String, CodeSet>>() {};
        	return  codeSet = yamlMapper.readValue(input.toFile(), typeRef);
        } catch (IOException e) {
            logger.atSevere().log("write coverage failed\n%s", e.getMessage());
        }
        return codeSet;
    }
}
