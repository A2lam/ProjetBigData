package esgi.exo

import io.univalence.sparktest.SparkTest
import org.scalatest.{FlatSpec, Matchers}

class FootballAppTest extends FlatSpec with Matchers with SparkTest {
  "convertToInt" should "return 0 when converting string NA" in {
    // Given
    val input = "NA"

    // When
    val result = FootballApp.convertToInt(input)

    // Then
    val expected = 0
    assert(result == expected)
  }

  "convertToInt" should "return 5 when converting string 5" in {
    // Given
    val input = "5"

    // When
    val result = FootballApp.convertToInt(input)

    // Then
    val expected = 5
    assert(result == expected)
  }
}
