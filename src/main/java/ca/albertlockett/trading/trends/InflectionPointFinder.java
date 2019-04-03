package ca.albertlockett.trading.trends;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.ta4j.core.Bar;
import org.ta4j.core.TimeSeries;

/**
 * Find supports and resistance points in a trendline
 */
public class InflectionPointFinder {

  private TimeSeries series;
  private boolean isSupport = true; // whether finding supports or resistance
  private int smoothingFactor = 1;  //
  private int clusterGap = 3;

  public InflectionPointFinder(TimeSeries series, boolean isSupport) {
    this.isSupport = isSupport;
    this.series = series;
  }

  public InflectionPointFinder(TimeSeries series, boolean isSupport, int smoothingFactor) {
    this.isSupport = isSupport;
    this.series = series;
    this.smoothingFactor = smoothingFactor;
  }

  public List<Integer> find() {
    List<Double> points = this.getPoints();
    List<Integer> reversalIndexes = this.findReversalIndexes(points);
    List<Integer> localExtremes = this.findExtremeInClusteredReversal(reversalIndexes);
    return localExtremes;
  }



  /**
   * Get list of points. Eventually, this could return list of close, open, high or low depending on how the class is
   * configured
   */
  private List<Double> getPoints() {
    return this.series.getBarData()
      .stream()
      .map(this::getPoint)
      .collect(Collectors.toList());
  }

  /**
   * Get single point from a bar. Same eventual goal as getPoints() from comment
   */
  public Double getPoint(Bar bar) {
    return bar.getMinPrice().doubleValue();
  }


  public Double calculateLHSAverageValueAtPoint(List<Double> points, int index) {
    if (index < this.smoothingFactor) {
      return null;
    }

    int j = 0;
    double[] values = new double[smoothingFactor];
    for (int i = index - 1 ; i >= index - this.smoothingFactor; i--) {
      values[j++] = points.get(i);
    }

    return DoubleStream.of(values).sum() / (double) this.smoothingFactor;
  }


  public Double calculateRHSAverageValueAtPoint(List<Double> points, int index) {
    if (index + this.smoothingFactor >= points.size()) {
      return null;
    }

    int j = 0;
    double[] values = new double[smoothingFactor];
    for (int i = index + 1; i <= index + smoothingFactor; i++) {
      values[j++] = points.get(i);
    }

    return DoubleStream.of(values).sum() / (double) this.smoothingFactor;
  }


  /**
   * find points where, looking forward/backwards and averaging, a reversal seems to have happened
   */
  private List<Integer> findReversalIndexes(List<Double> points) {
    
    List<Integer> reversalIndexes = new ArrayList<>();
    for (int i = 0; i < points.size(); i++) {
      Double lhsAverageValue = this.calculateLHSAverageValueAtPoint(points, i);
      if (lhsAverageValue == null) {
        continue;
      }

      Double rhsAverageValue = this.calculateRHSAverageValueAtPoint(points, i);
      if (rhsAverageValue == null) {
        continue;
      }

      double lhsSlope = (points.get(i) - lhsAverageValue) / this.smoothingFactor;
      double rhsSlope = (rhsAverageValue - points.get(i)) / this.smoothingFactor;

      if (this.isSupport) {
        if (lhsSlope < 0 && rhsSlope > 0) {
          reversalIndexes.add(i);
        }
      } else {
        if (lhsSlope > 0 && rhsSlope < 0) {
          reversalIndexes.add(i);
        }
      }
    }

    return reversalIndexes;
  }


  private Integer findIndexOfExtreme(List<Integer> indexes) {
    List<Double> points = indexes
      .stream()
      .map(i -> this.series.getBar(i))
      .map(this::getPoint)
      .collect(Collectors.toList());
    
    int extremeIndex = 0;
    for (int i = 1; i < points.size(); i++) {
      if (this.isSupport) {
        if (points.get(i) < points.get(extremeIndex)) {
          extremeIndex = i;
        }
      } else {
        if (points.get(i) > points.get(extremeIndex)) {
          extremeIndex = i;
        }
      }
    }

    return indexes.get(extremeIndex);
  }

  private List<Integer> findExtremeInClusteredReversal(List<Integer> reversalIndexes) {

    int clusterIndex = 0;
    List<List<Integer>> clusters = new ArrayList<>();
    clusters.add(new ArrayList<>());
    for (Integer index : reversalIndexes) {
      List<Integer> currentCluster = clusters.get(clusterIndex);
      
      // if this is the initial iteration, add to cluster
      if (clusterIndex == 0 && currentCluster.size() == 0) {
        currentCluster.add(index);
        continue;
      }

      Integer lastIndex = currentCluster.get(currentCluster.size() - 1);
      if (index - lastIndex <= this.clusterGap) {
        currentCluster.add(index);
        continue;
      }

      List<Integer> newCluster = new ArrayList<>();
      newCluster.add(index);
      clusters.add(newCluster);
      clusterIndex++;
    }

    return clusters
      .stream()
      .map(this::findIndexOfExtreme)
      .collect(Collectors.toList());
  }
}