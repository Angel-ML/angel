package com.tencent.angel.spark.ml.tree.sketch;

import java.util.Arrays;

/**
 * Implementation of quantile sketch on the Java heap
 * bashed on `DataSketches` of Yahoo!
 */
public class HeapQuantileSketch extends QuantileSketch {
    private int k; // parameter that controls space usage
    public static final int DEFAULT_K = 128;

    /**
     * This single array contains the base buffer plus all levels some of which may not be used.
     * A level is of size K and is either full and sorted, or not used. A "not used" buffer may have
     * garbage. Whether a level buffer used or not is indicated by the bitPattern_.
     * The base buffer has length 2*K but might not be full and isn't necessarily sorted.
     * The base buffer precedes the level buffers.
     *
     * The levels arrays require quite a bit of explanation, which we defer until later.
     */
    private float[] combinedBuffer;
    private int combinedBufferCapacity; // equals combinedBuffer.length
    private int baseBufferCount; // #samples currently in base buffer (= n % (2*k))
    private long bitPattern; // active levels expressed as a bit pattern (= n / (2*k))
    private static final int MIN_BASE_BUF_SIZE = 4;

    /**
     * data structure for answering quantile queries
     */
    private float[] samplesArr; // array of size samples
    private long[] weightsArr; // array of cut points

    public HeapQuantileSketch(int k, long estimateN) {
        super(estimateN);
        SketchUtils.checkK(k);
        this.k = k;
        reset();
    }

    public HeapQuantileSketch() {
        this(DEFAULT_K, -1L);
    }

    public HeapQuantileSketch(int k) {
        this(k, -1L);
    }

    public HeapQuantileSketch(long estimateN) {
        this(DEFAULT_K, estimateN);
    }

    @Override
    public void reset() {
        n = 0;
        if (estimateN < 0)
            combinedBufferCapacity = Math.min(MIN_BASE_BUF_SIZE, k * 2);
        else if (estimateN < k * 2)
            combinedBufferCapacity = k * 4;
        else
            combinedBufferCapacity = SketchUtils.needBufferCapacity(k, estimateN);
        combinedBuffer = new float[combinedBufferCapacity];
        baseBufferCount = 0;
        bitPattern = 0L;
        minValue = Float.MAX_VALUE;
        maxValue = Float.MIN_VALUE;
        samplesArr = null;
        weightsArr = null;
    }

    @Override
    public void update(float value) {
        if (Float.isNaN(value))
            throw new QuantileSketchException("Encounter NaN value");
        maxValue = Math.max(maxValue, value);
        minValue = Math.min(minValue, value);

        if (baseBufferCount + 1 > combinedBufferCapacity)
            ensureBaseBuffer();
        combinedBuffer[baseBufferCount++] = value;
        n++;
        if (baseBufferCount == (k * 2))
            fullBaseBufferPropagation();
    }

    private void ensureBaseBuffer() {
        final float[] baseBuffer = combinedBuffer;
        int oldSize = combinedBufferCapacity;
        if (oldSize >= k * 2)
            throw new QuantileSketchException("Buffer over size");
        int newSize = Math.max(Math.min(k * 2, oldSize * 2), 1);
        combinedBufferCapacity = newSize;
        combinedBuffer = Arrays.copyOf(baseBuffer, newSize);
    }

    private void ensureLevels(long newN) {
        int numLevels = 1 + (63 - Long.numberOfLeadingZeros(newN / (k * 2)));
        int spaceNeeded = k * (numLevels + 2);
        if (spaceNeeded <= combinedBufferCapacity) return;
        final float[] baseBuffer = combinedBuffer;
        combinedBuffer = Arrays.copyOf(baseBuffer, spaceNeeded);
        combinedBufferCapacity = spaceNeeded;
    }

    private void fullBaseBufferPropagation() {
        ensureLevels(n);
        final float[] baseBuffer = combinedBuffer;
        Arrays.sort(baseBuffer, 0, baseBufferCount);
        inPlacePropagationUpdate(0, baseBuffer, 0);
        baseBufferCount = 0;
        SketchUtils.checkBitPattern(bitPattern, n, k);
    }

    private void inPlacePropagationUpdate(int beginLevel, final float[] buf, int bufBeginPos) {
        final float[] levelsArr = combinedBuffer;
        int endLevel = beginLevel;
        long tmp = bitPattern >>> beginLevel;
        while ((tmp & 1) != 0) { tmp >>>= 1; endLevel++; }
        SketchUtils.compactBuffer(buf, bufBeginPos, levelsArr, (endLevel + 2) * k, k);
        SketchUtils.levelwisePropagation(bitPattern, k, beginLevel, endLevel, buf, bufBeginPos, levelsArr);
        bitPattern += 1L << beginLevel;
    }

    public void makeSummary() {
        int baseBufferItems = (int)(n % (k * 2));
        SketchUtils.checkBitPattern(bitPattern, n, k);
        int validLevels = Long.bitCount(bitPattern);
        int numSamples = baseBufferItems + validLevels * k;
        samplesArr = new float[numSamples];
        weightsArr = new long[numSamples + 1];

        copyBuf2Arr(numSamples);
        SketchUtils.blockyMergeSort(samplesArr, weightsArr, numSamples, k);

        long cnt = 0L;
        for (int i = 0; i <= numSamples; i++) {
            long newCnt = cnt + weightsArr[i];
            weightsArr[i] = cnt;
            cnt = newCnt;
        }
    }

    private void copyBuf2Arr(int numSamples) {
        long weight = 1L;
        int cur = 0;
        long bp = bitPattern;

        // copy the highest levels
        for (int level = 0; bp != 0; level++, bp >>>= 1) {
            weight *= 2;
            if ((bp & 1) != 0) {
                int offset = k * (level + 2);
                for (int i = 0; i < k; i++) {
                    samplesArr[cur] = combinedBuffer[i + offset];
                    weightsArr[cur] = weight;
                    cur++;
                }
            }
        }

        // copy baseBuffer
        int startBlk = cur;
        for (int i = 0; i < baseBufferCount; i++) {
            samplesArr[cur] = combinedBuffer[i];
            weightsArr[cur] = 1L;
            cur++;
        }
        weightsArr[cur] = 0L;
        if (cur != numSamples)
            throw new QuantileSketchException("Missing items when copying buffer to array");
        Arrays.sort(samplesArr, startBlk, cur);
    }

    @Override
    public void merge(QuantileSketch other) {
        if (other instanceof HeapQuantileSketch) {
            merge((HeapQuantileSketch) other);
        } else {
            throw new QuantileSketchException("Cannot merge different " +
                    "kinds of quantile sketches");
        }
    }

    public void merge(HeapQuantileSketch other) {
        if (other == null || other.isEmpty()) return;
        if (other.k != this.k)
            throw new QuantileSketchException("Merge sketches with different k");
        SketchUtils.checkBitPattern(other.bitPattern, other.n, other.k);
        if (this.isEmpty()) {
            this.copy(other);
            return;
        }

        // merge two non-empty quantile sketches
        long totalN = this.n + other.n;
        for (int i = 0; i  < other.baseBufferCount; i++) {
            update(other.combinedBuffer[i]);
        }
        ensureLevels(totalN);

        final float[] auxBuf = new float[k * 2];
        long bp = other.bitPattern;
        for (int level = 0; bp != 0L; level++, bp >>>= 1) {
            if ((bp & 1L) != 0L) {
                inPlacePropagationMerge(level, other.combinedBuffer,
                        k * (level + 2), auxBuf, 0);
            }
        }

        this.n = totalN;
        this.maxValue = Math.max(this.maxValue, other.maxValue);
        this.minValue = Math.min(this.minValue, other.minValue);
        this.samplesArr = null;
        this.weightsArr = null;
    }

    private void inPlacePropagationMerge(int beginLevel, final float[] buf, int bufStart,
                                         final float[] auxBuf, int auxBufStart) {
        final float[] levelsArr = combinedBuffer;
        int endLevel = beginLevel;
        long tmp = bitPattern >>> beginLevel;
        while ((tmp & 1) != 0) { tmp >>>= 1; endLevel++; }
        System.arraycopy(buf, bufStart, levelsArr, k * (endLevel + 2), k);
        SketchUtils.levelwisePropagation(bitPattern, k, beginLevel, endLevel, auxBuf, auxBufStart, levelsArr);
        bitPattern += 1L << beginLevel;
    }

    public void copy(HeapQuantileSketch other) {
        this.n = other.n;
        this.minValue = other.minValue;
        this.maxValue = other.maxValue;
        if (this.estimateN == -1) {
            this.combinedBufferCapacity = other.combinedBufferCapacity;
            this.combinedBuffer = other.combinedBuffer.clone();
        } else if (other.combinedBufferCapacity > this.combinedBufferCapacity) {
            this.combinedBufferCapacity = other.combinedBufferCapacity;
            this.combinedBuffer = other.combinedBuffer.clone();
        } else {
            System.arraycopy(other.combinedBuffer, 0,
                    this.combinedBuffer, 0, other.combinedBufferCapacity);
        }
        this.baseBufferCount = other.baseBufferCount;
        this.bitPattern = other.bitPattern;
        if (other.samplesArr != null && other.weightsArr != null) {
            this.samplesArr = other.samplesArr.clone();
            this.weightsArr = other.weightsArr.clone();
        }
    }

    @Override
    public float getQuantile(float fraction) {
        SketchUtils.checkFraction(fraction);
        if (samplesArr == null || weightsArr == null)
            makeSummary();

        if (samplesArr.length == 0)
            return Float.NaN;

        if (fraction == 0.0f)
            return minValue;
        else if (fraction == 1.0f)
            return maxValue;
        else
            return getQuantileFromArr(fraction);
    }

    @Override
    public float[] getQuantiles(float[] fractions) {
        SketchUtils.checkFractions(fractions);
        if (samplesArr == null || weightsArr == null)
            makeSummary();

        float[] res = new float[fractions.length];
        if (samplesArr.length == 0) {
            Arrays.fill(res, Float.NaN);
            return res;
        }

        for (int i = 0; i < fractions.length; i++) {
            if (fractions[i] == 0.0f)
                res[i] = minValue;
            else if (fractions[i] == 1.0f)
                res[i] = maxValue;
            else
                res[i] = getQuantileFromArr(fractions[i]);
        }
        return res;
    }

    @Override
    public float[] getQuantiles(int evenPartition) {
        SketchUtils.checkEvenPartiotion(evenPartition);
        if (samplesArr == null || weightsArr == null)
            makeSummary();

        float[] res = new float[evenPartition];
        if (samplesArr.length == 0) {
            Arrays.fill(res, Float.NaN);
            return res;
        }

        res[0] = minValue;
        int index = 0;
        float curFrac = 0.0f;
        float step = 1.0f / evenPartition;
        for (int i = 1; i < evenPartition; i++) {
            curFrac += step;
            long rank = (long)(n * curFrac);
            rank = Math.min(rank, n - 1);
            int left = index, right = weightsArr.length - 1;
            while (left + 1 < right) {
                int mid = left + ((right - left) >> 1);
                if (weightsArr[mid] <= rank)
                    left = mid;
                else
                    right = mid;
            }
            res[i] = samplesArr[left];
            index = left;
        }
        return res;
    }

    private float getQuantileFromArr(float fraction) {
        long rank = (long)(n * fraction);
        if (rank == n) n--;
        int left = 0, right = weightsArr.length - 1;
        while (left + 1 < right) {
            int mid = left + ((right - left) >> 1);
            if (weightsArr[mid] <= rank)
                left = mid;
            else
                right = mid;
        }
        return samplesArr[left];
    }

    public int getK() {
        return k;
    }


}
