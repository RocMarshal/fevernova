package com.github.fevernova.io.hdfs.writer;


import com.github.fevernova.framework.common.FNException;


public class WriterFactory {


    public static final String HDFS_SEQUENCEFILETYPE = "SequenceFile";

    public static final String HDFS_DATASTREAMTYPE = "DataStream";

    public static final String HDFS_COMPSTREAMTYPE = "CompressedDataStream";

    public static final String HDFS_ORCFILETYPE = "OrcFile";


    public static AbstractHDFSWriter getHDFSWriter(String fileType) {

        String writerClassName = "com.github.fevernova.io.hdfs.writer.HDFS" + fileType;
        try {
            return (AbstractHDFSWriter) Class.forName(writerClassName).newInstance();
        } catch (InstantiationException e) {
            throw new FNException("Class : " + writerClassName + " , can not be instantiated", e);
        } catch (IllegalAccessException e) {
            throw new FNException("Class instantiation error : may call a private method", e);
        } catch (ClassNotFoundException e) {
            throw new FNException("can not find class : " + writerClassName, e);
        }
    }

}
