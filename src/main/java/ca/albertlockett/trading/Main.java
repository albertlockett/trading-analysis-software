package ca.albertlockett.trading;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.ta4j.core.TimeSeries;
import org.ta4j.core.indicators.SMAIndicator;
import org.ta4j.core.indicators.helpers.ClosePriceIndicator;
import org.ta4j.core.num.Num;

import ca.albertlockett.trading.dao.Data;

public class Main {

  public static void main(String[] args) {

    String from = "2007-01-01";
    String to = "2018-12-31";
    String symbol = "AAPL";

    Data data = new Data();
    TimeSeries series = data.fetchBarData(from, to, symbol);
    System.out.println(series);

    // Getting the close price of the ticks
    Num firstClosePrice = series.getBar(0).getClosePrice();
    System.out.println("First close price: " + firstClosePrice.doubleValue());
    // Or within an indicator:
    ClosePriceIndicator closePrice = new ClosePriceIndicator(series);
    // Here is the same close price:
    System.out.println(firstClosePrice.isEqual(closePrice.getValue(0))); // equal to firstClosePrice

    // Getting the simple moving average (SMA) of the close price over the last 5 ticks
    SMAIndicator shortSma = new SMAIndicator(closePrice, 5);
    // Here is the 5-ticks-SMA value at the 42nd index
    System.out.println("5-ticks-SMA value at the 42nd index: " + shortSma.getValue(5).doubleValue());

    // Getting a longer SMA (e.g. over the 30 last ticks)
    SMAIndicator longSma = new SMAIndicator(closePrice, 30);
    System.out.println("Longer SMA: " + longSma.getValue(5).doubleValue());
  }

}