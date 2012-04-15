package jcascalog.example;

import cascalog.StdoutTap;
import jcascalog.Api;
import jcascalog.Fields;
import jcascalog.Playground;
import jcascalog.Predicate;
import jcascalog.Subquery;
import jcascalog.op.LT;


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
}
