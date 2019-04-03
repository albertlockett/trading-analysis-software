package ca.albertlockett.trading;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.ta4j.core.Bar;
import org.ta4j.core.TimeSeries;
import org.ta4j.core.indicators.SMAIndicator;
import org.ta4j.core.indicators.helpers.ClosePriceIndicator;
import org.ta4j.core.num.Num;

import ca.albertlockett.trading.dao.Data;
import ca.albertlockett.trading.trends.InflectionPointFinder;

public class Main {

  public static void main(String[] args) {

    String from = "2018-01-01";
    String to = "2018-12-31";
    String symbol = "AAPL";

    Data data = new Data();
    TimeSeries series = data.fetchBarData(from, to, symbol);


    InflectionPointFinder inflectionPointFinder = new InflectionPointFinder(series, true, 30);
    List<Integer> indices = inflectionPointFinder.find();

    for (Integer index : indices) {
      Bar bar = series.getBar(index);
      System.out.println(index + ", " + bar.getSimpleDateName() + ", " + bar.getClosePrice().doubleValue());
    }
    

  }

}