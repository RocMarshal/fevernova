package com.github.fevernova.io.hdfs.writer;


import com.github.fevernova.io.data.type.MethodType;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.io.hdfs.orc.OrcTypeEnum;
import com.github.fevernova.task.dataarchive.data.ListData;
import com.github.fevernova.task.dataarchive.schema.ColumnInfo;
import com.github.fevernova.task.dataarchive.schema.SchemaData;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.List;
import java.util.Properties;


@Slf4j
public class HDFSOrcFile extends AbstractHDFSWriter {


    private Properties orcProps = new Properties();

    private int batchNum;

    private List<ColumnInfo> columnInfos;

    private TypeDescription schema;

    private org.apache.orc.Writer writer;

    private VectorizedRowBatch batch;

    private String tmpPathStr;

    private String targetPathStr;


    @Override
    public void configure(GlobalContext globalContext, TaskContext writerContext) {

        super.configure(globalContext, writerContext);
        this.batchNum = writerContext.getInteger("batchsize", 16384);
        TaskContext orcContext = new TaskContext("orc", writerContext.getSubProperties("orc."));
        orcContext.getParameters().entrySet().forEach(entry -> orcProps.put(entry.getKey(), entry.getValue()));
    }


    @Override public void open() throws IOException {

        this.tmpPathStr = assemblePath(this.baseTmpPath, ".orc");
        if (log.isInfoEnabled()) {
            log.info("ORC_WRITER : create or open tmp file path : " + this.tmpPathStr);
        }
        this.targetPathStr = assemblePath(this.basePath, ".orc");
        this.schema = initSchema();
        this.batch = this.schema.createRowBatch(this.batchNum);
        this.writer = getApacheOrcWriter(this.tmpPathStr);
        Path tmpPath = new Path(this.tmpPathStr);
        if (!super.fileSystem.isFile(tmpPath)) {
            super.fileSystem.createNewFile(tmpPath);
        }
    }


    private TypeDescription initSchema() {

        TypeDescription _schema = TypeDescription.createStruct();
        this.columnInfos.forEach(columnInfo -> {
            TypeDescription orcType = columnInfo.getOrcTypeEnum().toOrcTypeDescption();
            _schema.addField(columnInfo.getTargetColumnName(), orcType);
        });
        return _schema;
    }


    @Override public int writeData(Data event) throws IOException {

        ListData listData = (ListData) event;
        int row = this.batch.size++;
        ColumnInfo col = null;
        List<Pair<MethodType, Object>> values = listData.getValues();
        try {
            for (int i = 0; i < values.size(); i++) {
                col = this.columnInfos.get(i);
                col.getUData().from(values.get(i).getValue(), values.get(i).getKey());
                col.getOrcTypeEnum().setValue(this.batch.cols[i], row, col.getUData());
            }
        } catch (Exception e) {
            log.error("data type convert error. hive col name : " + col.getTargetColumnName(), e);
            Validate.isTrue(false);
        }
        return 1;
    }


    private org.apache.orc.Writer getApacheOrcWriter(String filePath) throws IOException {

        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(this.orcProps, super.configuration).setSchema(this.schema)
                .compress(CompressionKind.valueOf(this.codecName.toUpperCase()));
        return OrcFile.createWriter(new Path(filePath), writerOptions);
    }


    @Override public void sync() throws IOException {

        this.writer.addRowBatch(this.batch);
        this.batch.reset();
        this.writer.writeIntermediateFooter();
    }


    @Override public Pair<String, String> close() throws IOException {

        this.batch.reset();
        this.writer.close();
        return Pair.of(this.tmpPathStr, this.targetPathStr);
    }


    public void initColumnInfos(SchemaData schemaData) {

        if (this.columnInfos != null) {
            return;
        }
        this.columnInfos = Lists.newArrayList();
        try {
            for (ColumnInfo columnInfo : schemaData.getColumnInfos()) {
                this.columnInfos.add(ColumnInfo.builder().clazz(columnInfo.getClazz())
                                             .uData(columnInfo.getClazz().getConstructor(boolean.class).newInstance(true))
                                             .sourceColumnName(columnInfo.getSourceColumnName())
                                             .targetColumnName(columnInfo.getTargetColumnName())
                                             .targetTypeEnum(columnInfo.getTargetTypeEnum())
                                             .orcTypeEnum(OrcTypeEnum.findType(columnInfo.getTargetTypeEnum()))
                                             .build());
            }
        } catch (Exception e) {
            log.error("Init ColumnInfo Error : ", e);
            Validate.isTrue(false);
        }
    }

}
