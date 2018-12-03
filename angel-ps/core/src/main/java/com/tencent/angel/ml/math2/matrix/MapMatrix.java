package com.tencent.angel.ml.math2.matrix;

import com.tencent.angel.ml.math2.vector.Vector;

import java.util.HashMap;

public class MapMatrix<Vec extends Vector> extends Matrix{
  protected HashMap<Long, Vec> mapMatrix;

  public MapMatrix(){
  }

  public  MapMatrix(int matrixId, int clock, HashMap<Long, Vec> mapMatrix){
    this.matrixId = matrixId;
    this.clock = clock;
    this.mapMatrix = mapMatrix;
  }

  public  MapMatrix(HashMap<Long, Vec> mapMatrix){
    this(0,0, mapMatrix);
  }

  public Vector getRow(int idx){
    return mapMatrix.get(idx);
  }

  public Vector getRow(long idx){
    return mapMatrix.get(idx);
  }

  public HashMap<Long, Vec> getRows(int[] idx){
    HashMap<Long, Vec>  matrix = new HashMap();
    for (int id : idx){
      matrix.put((long)id, mapMatrix.get(id));
    }
    return matrix;
  }

  public HashMap<Long, Vec> getRows(long[] idx){
    HashMap<Long, Vec>  matrix = new HashMap();
    for (long id : idx){
      matrix.put(id, mapMatrix.get(id));
    }
    return matrix;
  }

  public HashMap<Long, Vec> getRows(){
    return mapMatrix;
  }

  public void setRow(int idx, Vec v){
    mapMatrix.put((long)idx, v);
  }

  public void setRow(long idx, Vec v){
    mapMatrix.put(idx, v);
  }

  public void setRows(int[] idx, Vec[] vectors){
    for(int i = 0; i< idx.length; i++){
      mapMatrix.put((long)idx[i], vectors[i]);
    }
  }

  public void setRows(long[] idx, Vec[] vectors){
    for(int i = 0; i< idx.length; i++){
      mapMatrix.put(idx[i], vectors[i]);
    }
  }

  public void setRows(HashMap<Long, Vec> matrix){
    mapMatrix.putAll(matrix);
  }


  @Override
  public Vector diag() {
    return null;
  }

  @Override
  public Vector getCol(int idx) {
    return null;
  }

  @Override
  public Vector dot(Vector other) {
    return null;
  }

  @Override
  public Vector transDot(Vector other) {
    return null;
  }

  @Override
  public Matrix iadd(int rowId, Vector other) {
    return null;
  }

  @Override
  public Matrix add(int rowId, Vector other) {
    return null;
  }

  @Override
  public Matrix isub(int rowId, Vector other) {
    return null;
  }

  @Override
  public Matrix sub(int rowId, Vector other) {
    return null;
  }

  @Override
  public Matrix imul(int rowId, Vector other) {
    return null;
  }

  @Override
  public Matrix mul(int rowId, Vector other) {
    return null;
  }

  @Override
  public Matrix idiv(int rowId, Vector other) {
    return null;
  }

  @Override
  public Matrix div(int rowId, Vector other) {
    return null;
  }

  @Override
  public Matrix iaxpy(int rowId, Vector other, double aplha) {
    return null;
  }

  @Override
  public Matrix axpy(int rowId, Vector other, double aplha) {
    return null;
  }

  @Override
  public Matrix iadd(Vector other) {
    return null;
  }

  @Override
  public Matrix add(Vector other) {
    return null;
  }

  @Override
  public Matrix isub(Vector other) {
    return null;
  }

  @Override
  public Matrix sub(Vector other) {
    return null;
  }

  @Override
  public Matrix imul(Vector other) {
    return null;
  }

  @Override
  public Matrix mul(Vector other) {
    return null;
  }

  @Override
  public Matrix idiv(Vector other) {
    return null;
  }

  @Override
  public Matrix div(Vector other) {
    return null;
  }

  @Override
  public Matrix iaxpy(Vector other, double aplha) {
    return null;
  }

  @Override
  public Matrix axpy(Vector other, double aplha) {
    return null;
  }

  @Override
  public Matrix iadd(Matrix other) {
    return null;
  }

  @Override
  public Matrix add(Matrix other) {
    return null;
  }

  @Override
  public Matrix isub(Matrix other) {
    return null;
  }

  @Override
  public Matrix sub(Matrix other) {
    return null;
  }

  @Override
  public Matrix imul(Matrix other) {
    return null;
  }

  @Override
  public Matrix mul(Matrix other) {
    return null;
  }

  @Override
  public Matrix idiv(Matrix other) {
    return null;
  }

  @Override
  public Matrix div(Matrix other) {
    return null;
  }

  @Override
  public Matrix iaxpy(Matrix other, double aplha) {
    return null;
  }

  @Override
  public Matrix axpy(Matrix other, double aplha) {
    return null;
  }

  @Override
  public Matrix iadd(double x) {
    return null;
  }

  @Override
  public Matrix add(double x) {
    return null;
  }

  @Override
  public Matrix isub(double x) {
    return null;
  }

  @Override
  public Matrix sub(double x) {
    return null;
  }

  @Override
  public Matrix imul(double x) {
    return null;
  }

  @Override
  public Matrix mul(double x) {
    return null;
  }

  @Override
  public Matrix idiv(double x) {
    return null;
  }

  @Override
  public Matrix div(double x) {
    return null;
  }

  @Override
  public double min() {
    return 0;
  }

  @Override
  public double max() {
    return 0;
  }

  @Override
  public Vector min(int axis) {
    return null;
  }

  @Override
  public Vector max(int axis) {
    return null;
  }

  @Override
  public Vector sum(int axis) {
    return null;
  }

  @Override
  public Vector average(int axis) {
    return null;
  }

  @Override
  public Vector std(int axis) {
    return null;
  }

  @Override
  public Vector norm(int axis) {
    return null;
  }

  @Override
  public Matrix copy() {
    return null;
  }

  @Override
  public int getNumRows() {
    return 0;
  }

  @Override
  public void clear() {

  }

  @Override
  public double sum() {
    return 0;
  }

  @Override
  public double std() {
    return 0;
  }

  @Override
  public double average() {
    return 0;
  }

  @Override
  public double norm() {
    return 0;
  }
}
