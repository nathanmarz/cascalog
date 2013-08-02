package jcascalog.example;

import com.twitter.maple.tap.StdoutTap;

import jcascalog.Api;
import jcascalog.Option;
import jcascalog.Playground;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.GT;
import jcascalog.op.LT;
import jcascalog.op.Multiply;


public class Examples {
  public static void twentyFiveYearOlds() {
    Api.execute(new StdoutTap(), new Subquery("?person").predicate(Playground.AGE, "?person", 25));
  }

  public static void lessThanThirtyYearsOld() {
    Api.execute(new StdoutTap(), new Subquery("?person")
        .predicate(Playground.AGE, "?person", "?age").predicate(new LT(), "?age", 30));
  }

  public static void lessThanThirtyYearsOldWithAge() {
    Api.execute(new StdoutTap(), new Subquery("?person", "?age")
        .predicate(Playground.AGE, "?person", "?age").predicate(new LT(), "?age", 30));
  }

  public static void doubleAges() {
    Api.execute(new StdoutTap(), new Subquery("?person", "?double-age")
        .predicate(Playground.AGE, "?person", "?age").predicate(new Multiply(), "?age", 2)
        .out("?double-age"));
  }

  public static void distinctPeopleFromFollows() {
    Api.execute(new StdoutTap(), new Subquery("?person")
        .predicate(Playground.FOLLOWS, "?person", "_").predicate(Option.DISTINCT, true));
  }

  public static void nonDistinctPeopleFromFollows() {
    Api.execute(new StdoutTap(), new Subquery("?person")
        .predicate(Playground.FOLLOWS, "?person", "_"));
  }

  public static void malePeopleEmilyFollows() {
    Api.execute(new StdoutTap(), new Subquery("?person")
        .predicate(Playground.FOLLOWS, "emily", "?person")
        .predicate(Playground.GENDER, "?person", "m"));
  }

  public static void followsManyFollows() {
    Subquery manyFollows = new Subquery("?person").predicate(Playground.FOLLOWS, "?person", "_")
        .predicate(new Count(), "?count").predicate(new GT(), "?count", 2);
    Api.execute(new StdoutTap(), new Subquery("?person1", "?person2")
        .predicate(manyFollows, "?person1").predicate(manyFollows, "?person2")
        .predicate(Playground.FOLLOWS, "?person1", "?person2"));
  }

  public static void followsManyFollowsConcise() {
    // this implementation uses Api.each to shorten the implementation
    Subquery manyFollows = new Subquery("?person").predicate(Playground.FOLLOWS, "?person", "_")
        .predicate(new Count(), "?count").predicate(new GT(), "?count", 2);
    Api.execute(new StdoutTap(), new Subquery("?person1", "?person2")
        .predicate(Api.each(manyFollows), "?person1", "?person2")
        .predicate(Playground.FOLLOWS, "?person1", "?person2"));
  }

  public static void sentenceUniqueWords() {
    Api.execute(new StdoutTap(), new Subquery("?word").predicate(Playground.SENTENCE, "?sentence")
        .predicate(new Split(), "?sentence").out("?word").predicate(Option.DISTINCT, true));
  }

  public static void wordCount() {
    Api.execute(new StdoutTap(), new Subquery("?word", "?count")
        .predicate(Playground.SENTENCE, "?sentence").predicate(new Split(), "?sentence")
        .out("?word").predicate(new Count(), "?count"));
  }

  public static void lineCountWithFiles() {
    Api.execute(Api.hfsTextline("/tmp/myresults"), new Subquery("?count")
        .predicate(Api.hfsTextline("src/jvm/jcascalog/example"), "_")
        .predicate(new Count(), "?count"));
  }
}
