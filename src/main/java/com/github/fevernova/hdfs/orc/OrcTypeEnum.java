package com.github.fevernova.hdfs.orc;


import com.github.fevernova.data.type.UData;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.TypeDescription;


public enum OrcTypeEnum {

    TINYINT(new C_LongColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createByte();
        }
    }, SMALLINT(new C_LongColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createShort();
        }
    }, INT(new C_LongColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createInt();
        }
    }, BIGINT(new C_LongColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createLong();
        }
    }, BOOLEAN(new C_BooleanColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createBoolean();
        }
    }, FLOAT(new C_DoubleColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createFloat();
        }
    }, DOUBLE(new C_DoubleColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createDouble();
        }
    }, DECIMAL(new C_DecimalColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createDecimal();
        }
    }, STRING(new C_BytesColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createString();
        }
    }, BINARY(new C_BytesColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createBinary();
        }
    }, CHAR(new C_BytesColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createChar();
        }
    }, VARCHAR(new C_BytesColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createVarchar();
        }
    }, TIMESTAMP(new C_TimeStampColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createTimestamp();
        }
    }, DATE(new C_DateColumnVector()) {
        @Override
        public TypeDescription toOrcTypeDescption() {

            return TypeDescription.createDate();
        }
    };


    private Convert convert;


    OrcTypeEnum(Convert convert) {

        this.convert = convert;
    }


    public static OrcTypeEnum findType(String type) {

        return OrcTypeEnum.valueOf(type.toUpperCase());
    }


    public abstract TypeDescription toOrcTypeDescption();


    public void setValue(ColumnVector vector, int row, UData uData) {

        this.convert.eval(vector, row, uData);
    }

}
