package ch.heig.mac;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.driver.*;

public class Requests
{
    private static final Logger LOGGER = Logger.getLogger(Requests.class.getName());
    private final Driver driver;

    public Requests(Driver driver)
    {
        this.driver = driver;
    }

    public List<String> getDbLabels()
    {
        var dbVisualizationQuery = "CALL db.labels";

        try (var session = driver.session())
        {
            var result = session.run(dbVisualizationQuery);
            return result.list(t -> t.get("label").asString());
        }
    }

    public List<Record> possibleSpreaders()
    {
        var dbPossibleSpreadersQuery = "MATCH (sp:Person " +
                                       "{healthstatus:'Sick'})-[v1:VISITS]->(pl:Place) \n" +
                                       "WHERE sp.confirmedtime < v1.starttime AND EXISTS {     \n" +
                                       "    (hp:Person {healthstatus:'Healthy'})-[v2:VISITS]-(pl)   " +
                                       "    WHERE sp.confirmedtime < v2.starttime } \n" +
                                       "RETURN DISTINCT sp.name AS sickName";

        try (var session = driver.session())
        {
            var result = session.run(dbPossibleSpreadersQuery);
            return result.list();
        }
    }

    public List<Record> possibleSpreadCounts()
    {
        var dbPossibleSpreadCountsQuery = "MATCH (s:Person {healthstatus:'Sick'})-[v1:VISITS] - (pl:Place)-[v2:VISITS]-(h:Person {healthstatus:'Healthy'})\n" +
                                          "WHERE s.confirmedtime < v1.starttime AND s.confirmedtime < v2.starttime\n" +
                                          "RETURN s.name as sickName, count(h) as nbHealthy";
        try (var session = driver.session())
        {
            var result = session.run(dbPossibleSpreadCountsQuery);
            return result.list();
        }
    }

    public List<Record> carelessPeople()
    {
        var dbCarelessPeopleQuery = "MATCH (sp:Person {healthstatus:'Sick'})" +
                                    "-[v1:VISITS]-(pl:Place)  \n" +
                                    "RETURN sp.name AS sickName, count(DISTINCT pl.name) AS nbPlaces\n" +
                                    "ORDER BY nbPlaces DESC";

        try (var session = driver.session())
        {
            var result = session.run(dbCarelessPeopleQuery);
            return result.list();
        }
    }

    public List<Record> sociallyCareful()
    {
        var dbSociallyCarefulQuery = "MATCH (s:Person {healthstatus:'Sick'})\n" +
                                     "WHERE NOT EXISTS {\n" +
                                     "    (s)-[v:VISITS]-(pl:Place{type:'Bar'})\n" +
                                     "    WHERE s.confirmedtime > v.starttime\n" +
                                     "}\n" +
                                     "RETURN s.name as sickName";

        try (var session = driver.session())
        {
            var result = session.run(dbSociallyCarefulQuery);
            return result.list();
        }
    }

    public List<Record> peopleToInform()
    {
        var dbPeopleToInform = "MATCH (sp:Person {healthstatus:'Sick'})-[vsp:VISITS]->(:Place)<-[vhp:VISITS]-(hp:Person {healthstatus:'Healthy'})\n" +
                               "WHERE (datetime() + duration.between(apoc.coll.max([vsp.starttime, vhp.starttime]), apoc.coll.min([vsp.endtime, vhp.endtime]))) >= (datetime() + duration({hours:2}))\n" +
                               "RETURN sp.name AS sickName, collect(DISTINCT hp.name) AS peopleToInform";

        try (var session = driver.session())
        {
            var result = session.run(dbPeopleToInform);
            return result.list();
        }
    }

    public List<Record> setHighRisk()
    {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> healthyCompanionsOf(String name)
    {
        Map<String, Object> params = new HashMap<>();
        params.put("name", name);


        var dbHealthyCompanionOfQuery = "MATCH (tp:Person {name: $name})" +
                                        "-[:VISITS*2..6]-(hp:Person {healthstatus: \"Healthy\"})\n" +
                                        "RETURN DISTINCT hp.name AS healthyName";


        try (var session = driver.session())
        {
            var result =
                    session.run(dbHealthyCompanionOfQuery, params);
            return result.list();
        }
    }

    public Record topSickSite()
    {
        var dbTopSickSite = "MATCH (p:Person {healthstatus:'Sick'})-[v:VISITS]-(pl:Place)\n" +
                            "WHERE p.confirmedtime < v.starttime\n" +
                            "RETURN pl.type as placeType, COUNT(*) as nbOfSickVisits\n" +
                            "ORDER BY nbOfSickVisits DESC\n" +
                            "LIMIT 1";

        try (var session = driver.session())
        {
            var result = session.run(dbTopSickSite);
            return result.single();
        }
    }

    public List<Record> sickFrom(List<String> names)
    {
        Map<String,Object> params = new HashMap<>();
        params.put( "names", names );

        var dbSickFrom = "MATCH (p:Person {healthstatus:'Sick'})\n" +
                         "WHERE p.name IN $names\n" +
                         "RETURN p.name AS sickName";

        try (var session = driver.session())
        {
            var result = session.run(dbSickFrom, params);
            return result.list();
        }
    }
}
