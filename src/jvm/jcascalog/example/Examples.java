package jcascalog.example;

import cascalog.StdoutTap;
import jcascalog.Api;
import jcascalog.Fields;
import jcascalog.Option;
import jcascalog.Playground;
import jcascalog.Predicate;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.LT;
import jcascalog.op.Multiply;


public class Examples {
    public static void twentyFiveYearOlds() {
        Api.execute(
          new StdoutTap(),
          new Subquery(new Fields("?person"),
            new Predicate(Playground.AGE, new Fields("?person", 25))
            ));
    }

    public static void lessThanThirtyYearsOld() {
        Api.execute(
          new StdoutTap(),
          new Subquery(new Fields("?person"),
            new Predicate(Playground.AGE, new Fields("?person", "?age")),
            new Predicate(new LT(), new Fields("?age", 30))
            ));
    }

    public static void lessThanThirtyYearsOldWithAge() {
        Api.execute(
          new StdoutTap(),
          new Subquery(new Fields("?person", "?age"),
            new Predicate(Playground.AGE, new Fields("?person", "?age")),
            new Predicate(new LT(), new Fields("?age", 30))
            ));
    }

    public static void doubleAges() {
        Api.execute(
          new StdoutTap(),
          new Subquery(new Fields("?person", "?age", "?double-age"),
            new Predicate(Playground.AGE, new Fields("?person", "?age")),
            new Predicate(new Multiply(), new Fields("?age", 2), new Fields("?double-age"))
            ));
    }

    public static void distinctPeopleFromFollows() {
        Api.execute(
          new StdoutTap(),
          new Subquery(new Fields("?person"),
            new Predicate(Playground.FOLLOWS, new Fields("?person", "_"))
            ));
    }

    public static void nonDistinctPeopleFromFollows() {
        Api.execute(
          new StdoutTap(),
          new Subquery(new Fields("?person"),
            new Predicate(Playground.FOLLOWS, new Fields("?person", "_")),
            Option.distinct(false)
            ));
    }
    
    public static void malePeopleEmilyFollows() {
        Api.execute(
          new StdoutTap(),
          new Subquery(new Fields("?person"),
            new Predicate(Playground.FOLLOWS, new Fields("emily", "?person")),
            new Predicate(Playground.GENDER, new Fields("?person", "m"))
            ));
    }
    
    public static void sentenceUniqueWords() {
        Api.execute(
          new StdoutTap(),
          new Subquery(new Fields("?word"),
            new Predicate(Playground.SENTENCE, new Fields("?sentence")),
            new Predicate(new Split(), new Fields("?sentence"), new Fields("?word"))
            ));
    }

    public static void wordCount() {
        Api.execute(
          new StdoutTap(),
          new Subquery(new Fields("?word", "?count"),
            new Predicate(Playground.SENTENCE, new Fields("?sentence")),
            new Predicate(new Split(), new Fields("?sentence"), new Fields("?word")),
            new Predicate(new Count(), new Fields("?count"))
            ));
    }
}
