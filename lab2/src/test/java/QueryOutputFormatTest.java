import ch.heig.mac.Main;
import ch.heig.mac.Requests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryOutputFormatTest {

    private Driver driver;
    private Requests requests;

    @BeforeEach
    public void setUp() {
        driver = Main.openConnection();
        requests = new Requests(driver);
    }


    @AfterEach
    public void tearDown() {
        driver.close();
    }

    @Test
    public void testGetDbLabelsQuery() {
        assertThat(requests.getDbLabels())
                .hasSameElementsAs(List.of("_Bloom_Perspective_", "Person", "Place", "Visit", "Region", "Country", "Continent"));
    }

    @Test
    public void testPossibleSpreadersQuery() {
        assertThat(requests.possibleSpreaders().get(0).keys())
                .hasSameElementsAs(List.of("sickName"));
    }

    @Test
    public void testPossibleSpreadCountsQuery() {
        assertThat(requests.possibleSpreadCounts().get(0).keys())
                .hasSameElementsAs(List.of("sickName", "nbHealthy"));
    }

    @Test
    public void testCarelessPeopleQuery() {
        assertThat(requests.carelessPeople().get(0).keys())
                .hasSameElementsAs(List.of("sickName", "nbPlaces"));
    }

    @Test
    public void testSociallyCarefulQuery() {
        assertThat(requests.sociallyCareful().get(0).keys())
                .hasSameElementsAs(List.of("sickName"));
    }

    @Test
    public void testPeopleToInformQuery() {
        assertThat(requests.peopleToInform().get(0).keys())
                .hasSameElementsAs(List.of("sickName", "peopleToInform"));
    }

    @Test
    public void testSetHighRiskQuery() {
        assertThat(requests.setHighRisk().get(0).keys())
                .hasSameElementsAs(List.of("highRiskName"));
    }

    @Test
    public void testHealthyCompanionsOfQuery() {
        assertThat(requests.healthyCompanionsOf("Rocco Mendez").get(0).keys())
                .hasSameElementsAs(List.of("healthyName"));
    }

    @Test
    public void testTopSickSiteQuery() {
        assertThat(requests.topSickSite().keys())
                .hasSameElementsAs(List.of("placeType", "nbOfSickVisits"));
    }

    @Test
    public void testSickFromQuery() {
        assertThat(requests.sickFrom(List.of("Landyn Greer", "Saniyah Fuller", "Baylee Leblanc")).get(0).keys())
                .hasSameElementsAs(List.of("sickName"));
    }
}
