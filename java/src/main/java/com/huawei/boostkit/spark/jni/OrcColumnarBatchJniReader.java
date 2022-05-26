/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
package com.huawei.boostkit.spark.jni;

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.orc.OrcFile.ReaderOptions;
import org.apache.orc.Reader.Options;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.orc.TypeDescription;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class OrcColumnarBatchJniReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrcColumnarBatchJniReader.class);

    public long reader;
    public long recordReader;
    public long batchReader;
    public int[] colsToGet;
    public int realColsCnt;

    public OrcColumnarBatchJniReader(){
        NativelLoader.getINSTANCE();
    }

    public JSONObject getSubJson (ExpressionTree etNode){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("op", etNode.getOperator().ordinal());
        if(etNode.getOperator().toString().equals("LEAF")){
            jsonObject.put("leaf", etNode.toString());
            return jsonObject;
        }
        ArrayList<JSONObject> child = new ArrayList<JSONObject>();
        for (ExpressionTree childNode : etNode.getChildren()){
            JSONObject rtnJson = getSubJson(childNode);
            child.add(rtnJson);
        }
        jsonObject.put("child", child);
        return jsonObject;
    }

    public JSONObject getLeavesJson(List<PredicateLeaf> leaves, TypeDescription schema){
        JSONObject jsonObjectList = new JSONObject();
        for (int i = 0; i < leaves.size(); i++){
            PredicateLeaf p1 = leaves.get(i);
            JSONObject jsonObject = new JSONObject;
            jsonObject.put("op", p1.getOperator().ordinal());
            jsonObject.put("name", p1.getColumnName());
            jsonObject.put("type", p1.getType().ordinal());
            if(p1.getLiteral() != null){
                if (p1.getType() == PredicateLeaf.Type.DATE){
                    jsonObject.put("literal",((int)Math.ceil(((Date).getLiteral()).getTime()* 1.0/3600/24/1000)) + "");
                } else if (p1.getType() == PredicateLeaf.Type.DECIMAL){
                    int decimalP =  schema.findSubtype(p1.getColumnName()).getPrecsion();
                    int decimalS =  schema.findSubtype(p1.getColumnName()).getScale();
                    jsonObject.put("literal", p1.getLiteral().toString() + " " + decimalP + " " + decimalS);
                } else {
                    jsonObject.put("literal", p1.getLiteral().toString());
                }
            } else {
                jsonObject.put("literal", "");
            }
            if((p1.getLiteralList() != null) && (p1.getLiteralList().size() != 0)){
                ArrayList<String> lst = new ArrayList<String>();
                for (Object ob : p1.getLiteralList){
                    if (p1.getType() == PredicateLeaf.Type.DECIMAL){
                        int decimalP =  schema.findSubtype(p1.getColumnName()).getPrecsion();
                        int decimalS =  schema.findSubtype(p1.getColumnName()).getScale();
                        lst.add(ob.toString() + " " + decimalP + " " + decimalS);
                    }else if (p1.getType() == PrediateLeaf.Type.Date){
                        lst.add(((int)Math.ceil(((Date).getLiteral()).getTime()* 1.0/3600/24/1000)) + "");
                    }else {
                        lst.add(ob.toString());
                    }
                }
                jsonObject.put("literalList", lst);
            }else {
                jsonObject.put("literalList", new ArrayList<String>());
            }
            jsonObjectList.put("leaf-" + i, jsonObject);
        }
        return jsonObjectList;
    }

    /**
     * Init Orc reader.
     *
     * @param path split file path
     * @param options split file options
     */
    public long initializeReaderJava(String path, ReaderOptions options) {
        JSONObjecct job = new JSONObjecct();
        if(options.getOrcTail() == null){
            job.put("serializedTail", "");
        }else {
            job.put("serializedTail", options.getOrTail().getSerializedTail().toString());
        }
        job.put("tailLocation", 9223372036854775807L);
        reader = initializeReaderJava(path, job);
        return reader;
    }

    /**
     * Init Orc RecordReader.
     * @param options split file options
     */
    public long initializeRecordReaderJava(Options options){
        JSONObject job = new JSONObject();
        if(options.getInClude() == null){
            job.put("include", "");
        }else {
            job.put("include", options.getInclude().toString());
        }
        job.put("offset", options.getOffset());
        job.put("length", options.getLength());
        if (options.getSearchArgument() != null){
            LOGGER.debug("SearchArgument:" + options.getSearhArgument().toString());
            JSONObject jsonexpressionTree = getSubJson(options.getSearchArgument().getExpression());
            job.put("expressionTree", jsonexpressionTree);
            JSONObject jsonleaves = getLeavesJson(options.getSearchArgument().getLeaves(), options.getSchema());
            job.put("leaves", jsonleaves);
        }

        List<String> allCols;
        if (options.getColumnNames() == null){
            allCols = Arrays.asList(getAllColumnNames(reader));
        }else {
            allCols = Arrays.asList(options.getColumnNames());
        }
        ArrayList<String> colToInclu = new ArrayList<String>();
        List<String> optionField = options.getSchema().getFieldNames();
        colsToGet = new int[optionField.size()];
        realColsCnt = 0;
        for (int i=0; i < optionField.size(); i++){
            if (allCols.contains(optionField.get(i))){
                colToInclu.add(optionField.get(i));
                colsToGet[i] = 0;
                realColsCnt++;
            }else {
                colsToGet[i] = -1;
            }
        }
        job.put("includedColumns", colToInclu.toArray());
        recordReader = initializeRecordReader(recordReader, batchReader);
        return recordReader;
    }

    public long initBatchJava(long batchSize){
        batchReader = initializeBatch(recordReader, batchSize);
        return 0;
    }

    public long getNumberOfRowsJava(){
        return getNumberOfRows(recordReader, batchReader);
    }

    public long getRowNumber(){
        return recordReaderGetRowNumber(recordReader);
    }

    public float getProgress(){
        return recordReaderGetProgress(recordReader);
    }

    public void close(){
        recordReaderClose(recordReader, reader, batchReader);
    }

    public void seekToRow(long rowNumber){
        recordReaderSeekToRow(recordReader,rowNumber);
    }

    public int next(Vec[] vecList){
        int vectorCnt = vecList.length;
        int[] typeIds = new int[realColsCnt];
        long[] vecNativeIds = new long[realColsCnt];
        long rtn = recordReaderNext(recordReader, reader, batchReader, typeIds, vecNativeIds);
        if (rtn == 0){
            return 0;
        }
        int nativeGetId = 0;
        for (int i = 0; i < vectorCnt; i++){
            if (colsToGet[i] != 0){
                continue;
            }
            switch (DataType.DataTypeId.values()[typeIds[nativeGetId]]){
                case OMNI_DATE32:
                case OMNI_INT:{
                    vecList[i] = new IntVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_LONG:{
                    vecList[i] = new LongVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_VARCHAR:{
                    vecList[i] = new VarcharVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_DECIMAL128:{
                    vecList[i] = new Decimal128Vec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_DECIMAL64:{
                    vecList[i] = new LongVec(vecNativeIds[nativeGetId]);
                    break;
                }
                default:{
                    LOGGER.error("UNKNIWN TYPE ERROR IN JAVA" + DataType.DataTypeId.values()[typeIds[i]]);
                }
            }
            nativeGetId++;
        }
        return (int)rtn;
    }

    public native long initializeReader(String path, JSONObject job);

    public native long initializeRecordReader(long reader, JSONObject job);

    public native long initializeBatch(long rowReader, long batchSize);

    public native long recordReaderNext(long rowReader, long reader, long batchReader, int[] typeId, long[] vecNativeId);

    public native long recordReaderGetRowNumber(long rowReader);

    public native float recordReaderGetProgress(long rowReader);

    public native void recordReaderClose(long rowReader, long reader, long batchReader);

    public native void recordReaderSeekToRow(long rowReader, long rowNumber);

    public native String[] getAllColumnNames(long reader);

    public native long getNumberOfRows(long rowReader, long batch);
}
