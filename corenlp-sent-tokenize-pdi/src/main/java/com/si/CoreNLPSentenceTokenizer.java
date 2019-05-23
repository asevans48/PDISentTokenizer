/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.si;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;


/**
 * Describe your step plugin.
 * 
 */
public class CoreNLPSentenceTokenizer extends BaseStep implements StepInterface {
  private CoreNLPSentenceTokenizerMeta meta;
  private CoreNLPSentenceTokenizerData data;

  private static Class<?> PKG = CoreNLPSentenceTokenizerMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  
  public CoreNLPSentenceTokenizer( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }
  
  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param stepMetaInterface     The metadata to work with
   * @param stepDataInterface     The data to initialize
   */
  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    this.meta = (CoreNLPSentenceTokenizerMeta) stepMetaInterface;
    this.data = (CoreNLPSentenceTokenizerData) stepDataInterface;
    return super.init( stepMetaInterface, stepDataInterface );
  }

  /**
   * Package the row
   * @param rowMeta       The row meta interface
   * @param sentence      The sentence
   * @param r             An array representing the resulting row
   * @return              The updated row
   */
  private Object[] packageRow(RowMetaInterface rowMeta, String sentence, Object[] r){
    int idx = rowMeta.indexOfValue(meta.getOutField());
    if(idx >= 0){
      r[idx] = sentence.trim();
    }else{
      if(isBasic()){
        logBasic("Output Field for Sentence Splitter Does Not Exist!");
      }
    }
    return r;
  }

  /**
   * Split a sentence to different sentences.
   *
   * @param rowMeta           The input row meta
   * @param splitText         The text to split
   * @param r                 The object array row representation
   * @return                  The packaged rows per sentence
   */
  private ArrayList<Object[]> splitSentence(RowMetaInterface rowMeta, String splitText, Object[] r){
    ArrayList<Object[]> orows = new ArrayList<Object[]>();
    try (StringReader reader = new StringReader(splitText)) {
      DocumentPreprocessor dp = new DocumentPreprocessor(reader);
      for(List<HasWord> sentenceList : dp){
        String sentence = "";
        for(HasWord word : sentenceList){
          String text = word.word();
          sentence = sentence + " " + text;
        }
        if(sentence != null && sentence.trim().length() > 0) {
          Object[] orow = packageRow(rowMeta, sentence.trim(), r.clone());
          orows.add(orow);
        }
      }
    }
    return orows;
  }

  /**
   * Get sentences from the row.
   *
   * @param rowMeta       The input row meta
   * @param r             The object row array
   * @return              The row
   */
  private ArrayList<Object[]> getSentences(RowMetaInterface rowMeta, Object[] r){
    ArrayList<Object[]> orows = new ArrayList<>();
    int idx = rowMeta.indexOfValue(meta.getInField());
    if(idx >= 0) {
      String splitText = (String) r[idx];
      if(splitText != null && splitText.length() > 0) {
        ArrayList<Object[]> rows = splitSentence(rowMeta, splitText, r);
        orows.addAll(rows);
      }
    }else{
      if(isBasic()){
        logBasic("Input Field Index Not Found for Sentence Tokenizer");
      }
    }
    return orows;
  }

  /**
   * Check the row meta to ensure that all fields exist.
   *
   * @param rmi         The row meta interface
   * @return            The updated row meta interface
   */
  public RowMetaInterface processRowMeta(RowMetaInterface rmi) throws KettleException{
    String[] fields = rmi.getFieldNames();
    String[] fieldnames = {meta.getOutField(), };

    int idx = stringArrayContains(fields, meta.getOutField());
    if(idx == -1){
      throw new KettleException("Sent Tokenizer missing output field");
    }

    for(int i = 0; i < fieldnames.length; i++){
      String fname = fieldnames[i];
      int cidx = stringArrayContains(fields, fname);
      if(cidx == -1){
        ValueMetaInterface value = ValueMetaFactory.createValueMeta(fname, ValueMetaInterface.TYPE_STRING);
        rmi.addValueMeta(value);
      }
    }
    return rmi;
  }

  /**
   * Check if the value exists in the array
   *
   * @param arr  The array to check
   * @param v    The value in the array
   * @return  Whether the value exists
   */
  private int stringArrayContains(String[] arr, String v){
    int exists = -1;
    int i = 0;
    while(i < arr.length && exists == -1){
      if(arr[i].equals(v)){
        exists = i;
      }else {
        i += 1;
      }
    }
    return exists;
  }

  /**
   * Setup the processor.
   *
   * @throws KettleException
   */
  private void setupProcessor() throws KettleException{
    RowMetaInterface inMeta = getInputRowMeta().clone();
    data.outputRowMeta = inMeta;
    meta.getFields(data.outputRowMeta, getStepname(), null, null, this, null, null);
    data.outputRowMeta = processRowMeta(data.outputRowMeta);
    first = false;
  }

  /**
   * Output rows to next step.
   *
   * @param orows                   The rows
   * @param r                       The object array row representation
   * @throws KettleException
   */
  private void outputRows(ArrayList<Object[]> orows, Object[] r) throws KettleException{
    if(orows.size() > 0) {
      for(Object[] row : orows){
        putRow(data.outputRowMeta, row);
      }
    }else{
      if(data.outputRowMeta.size() > r.length){
       Object[] rsz = RowDataUtil.resizeArray(r, data.outputRowMeta.size());
       putRow(data.outputRowMeta, rsz);
      }else {
        putRow(data.outputRowMeta, r);
      }
    }
  }

  /**
   * Process an incoming row.
   *
   * @param smi                 The step meta interface
   * @param sdi                 The step data interface
   * @return                    Whether a row was processed.
   * @throws KettleException
   */
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    Object[] r = getRow();
    if ( r == null ) {
      setOutputDone();
      return false;
    }

    if(first){
      setupProcessor();
    }

    r = RowDataUtil.resizeArray(r, data.outputRowMeta.size());
    ArrayList<Object[]> orows = getSentences(data.outputRowMeta, r);
    outputRows(orows, r);

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() )
        logBasic( BaseMessages.getString( PKG, "CoreNLPSentenceTokenizer.Log.LineNumber" ) + getLinesRead() );
    }
    return true;
  }
}