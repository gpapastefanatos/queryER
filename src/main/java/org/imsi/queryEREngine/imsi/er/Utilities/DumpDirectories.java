package org.imsi.queryEREngine.imsi.er.Utilities;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class DumpDirectories {
    private static String dataDirPath;
    private static String logsDirPath;
    private static String blockDirPath;
    private static String blockIndexDirPath;
    private static String groundTruthDirPath;
    private static String tableStatsDirPath;
    private static String blockIndexStatsDirPath;
    private static String linksDirPath;

    public DumpDirectories(String dumpPath){
        dataDirPath = dumpPath;
        logsDirPath = dumpPath + "/logs";
        blockDirPath = dumpPath + "/blocks";
        blockIndexDirPath = dumpPath + "/blockIndex";
        groundTruthDirPath = dumpPath + "/groundTruth";
        tableStatsDirPath = dumpPath + "/tableStats/tableStats";
        blockIndexStatsDirPath = dumpPath + "/tableStats/blockIndexStats";
        linksDirPath = dumpPath + "/links";
    }

    public static void generateDumpDirectories() throws IOException {
        File dataDir = new File(dataDirPath);
        File logsDir = new File(logsDirPath);
        File blockDir = new File(blockDirPath);
        File blockIndexDir = new File(blockIndexDirPath);
        File groundTruthDir = new File(groundTruthDirPath);
        File tableStatsDir = new File(tableStatsDirPath);
        File blockIndexStats = new File(blockIndexStatsDirPath);
        File linksDir = new File(linksDirPath);
        if(!dataDir.exists()) {
            FileUtils.forceMkdir(dataDir); //create directory
        }
        if(!logsDir.exists()) {
            FileUtils.forceMkdir(logsDir); //create directory
        }
        if(!blockIndexDir.exists()) {
            FileUtils.forceMkdir(blockIndexDir); //create directory
        }
        if(!groundTruthDir.exists()) {
            FileUtils.forceMkdir(groundTruthDir); //create directory
        }
        if(!tableStatsDir.exists()) {
            FileUtils.forceMkdir(tableStatsDir); //create directory
        }
        if(!blockIndexStats.exists()) {
            FileUtils.forceMkdir(blockIndexStats); //create directory
        }
        if(!linksDir.exists()) {
            FileUtils.forceMkdir(linksDir); //create directory
        }
        if(!blockDir.exists()) {
            FileUtils.forceMkdir(blockDir); //create directory
        }

    }

    public static String getDataDirPath() {
        return dataDirPath;
    }

    public static String getLogsDirPath() {
        return logsDirPath;
    }

    public static String getBlockDirPath() {
        return blockDirPath;
    }

    public static String getBlockIndexDirPath() {
        return blockIndexDirPath;
    }

    public static String getGroundTruthDirPath() {
        return groundTruthDirPath;
    }

    public static String getTableStatsDirPath() {
        return tableStatsDirPath;
    }

    public static String getBlockIndexStatsDirPath() {
        return blockIndexStatsDirPath;
    }

    public static String getLinksDirPath() {
        return linksDirPath;
    }
}
