package com.github.fevernova.hdfs.writer;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.hdfs.PartitionType;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.lzo.LzopCodec;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public abstract class AbstractHDFSWriter implements Writer {


    protected Configuration configuration;

    protected FileSystem fileSystem;

    protected String codecName;

    protected String basePath;

    protected String baseTmpPath;

    protected PartitionType.PartitionRender partitionRender;

    @Setter
    private int index;

    private String userName;

    protected PartitionType partitionType;

    protected GlobalContext globalContext;


    @Override
    public void configure(GlobalContext globalContext, TaskContext hdfsContext) {

        this.globalContext = globalContext;
        this.userName = hdfsContext.getString("username");
        Validate.notBlank(this.userName);
        System.setProperty("HADOOP_USER_NAME", this.userName);

        String hdfsConfigPath = hdfsContext.get("hdfsconfigpath");
        Validate.notBlank(hdfsConfigPath);

        this.configuration = new Configuration();
        this.configuration.addResource(hdfsConfigPath + "/core-site.xml");
        this.configuration.addResource(hdfsConfigPath + "/hdfs-site.xml");
        try {
            this.fileSystem = FileSystem.get(this.configuration);
        } catch (IOException e) {
            log.error("HDFS File System Error : ", e);
            Validate.isTrue(false);
        }

        this.codecName = hdfsContext.getString("codec", "snappy");

        this.basePath = hdfsContext.getString("basepath");
        this.baseTmpPath = hdfsContext.getString("basetmppath");

        this.partitionType = PartitionType.valueOf(hdfsContext.getString("partitiontype", PartitionType.HOUR.name()).toUpperCase());
        this.partitionRender = this.partitionType.newInstance(hdfsContext.getInteger("period", 1));
    }


    protected String assemblePath(String path, String codecFileExtension) {

        StringBuilder pathBuilder = new StringBuilder(path);
        this.partitionRender.render(pathBuilder);
        pathBuilder.append(this.globalContext.getJobTags().getJobId()).append("-")
                .append(this.globalContext.getJobTags().getPodIndex()).append("-")
                .append(this.index).append("-")
                .append(Util.nowMS()).append("-")
                .append(codecFileExtension);
        if (log.isDebugEnabled()) {
            log.debug("assemble path : {}", pathBuilder.toString());
        }
        return pathBuilder.toString();
    }


    protected boolean needCompressor() {

        return !"lzop".equals(this.codecName);
    }


    protected static boolean codecMatches(Class<? extends CompressionCodec> cls, String codecName) {

        String simpleName = cls.getSimpleName();
        if (cls.getName().equals(codecName) || simpleName.equalsIgnoreCase(codecName)) {
            return true;
        }
        if (simpleName.endsWith("Codec")) {
            String prefix = simpleName.substring(0, simpleName.length() - "Codec".length());
            if (prefix.equalsIgnoreCase(codecName)) {
                return true;
            }
        }
        return false;
    }


    protected CompressionCodec getCodec() {

        Configuration conf = new Configuration();
        List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory.getCodecClasses(conf);
        codecs.add(LzoCodec.class);
        codecs.add(LzopCodec.class);
        CompressionCodec codec = null;
        List<String> codecStrs = new ArrayList<>();
        codecStrs.add("NONE");
        for (Class<? extends CompressionCodec> cls : codecs) {
            codecStrs.add(cls.getSimpleName());
            if (codecMatches(cls, this.codecName)) {
                try {
                    codec = cls.newInstance();
                    if (codec instanceof Configurable) {
                        ((Configurable) codec).setConf(conf);
                    }
                } catch (InstantiationException e) {
                    log.error("Unable to instantiate " + cls + " class");
                } catch (IllegalAccessException e) {
                    log.error("Unable to access " + cls + " class");
                }
            }
        }
        if (codec == null) {
            if (!codecName.equalsIgnoreCase("None")) {
                throw new IllegalArgumentException("Unsupported compression codec " + codecName + ".  Please choose from: " + codecStrs);
            }
        }
        return codec;
    }


    public boolean releaseDataFile(String inputPathStr, String outputPathStr) throws IOException {

        Path inputPath = new Path(inputPathStr);
        if (this.fileSystem.exists(inputPath)) {
            Path outputPath = new Path(outputPathStr);
            Path parent = outputPath.getParent();
            if (!this.fileSystem.exists(parent)) {
                this.fileSystem.mkdirs(parent);
            } else if (!this.fileSystem.isDirectory(parent)) {
                this.fileSystem.delete(parent, true);
                this.fileSystem.mkdirs(parent);
            }
            if (this.fileSystem.exists(outputPath)) {
                log.warn(outputPath + " is exist, will be deleted");
                this.fileSystem.delete(outputPath, true);
            }
            return this.fileSystem.rename(inputPath, outputPath);
        }
        return false;
    }

}
