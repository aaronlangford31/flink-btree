package flink.state.BTreeState.util

import java.util.function.BiFunction

import flink.state.BTreeState.util.Search.Comparison
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConversions._
import org.scalatest.wordspec.AnyWordSpec

class SearchSpec extends AnyWordSpec with Matchers {
  val compare = new BiFunction[Int, Int, Comparison] {
    override def apply(t: Int, u: Int): Comparison = t.compareTo(u) match {
      case x if x == 0 => Comparison.EQUAL
      case x if x < 0 => Comparison.LESS_THAN
      case x if x > 0 => Comparison.GREATER_THAN
    }
  }

  "Search" when {
    "doing a search over single element collection" should {
      val singleElList = seqAsJavaList(Seq(44))
      val search = new Search[Int](singleElList)

      "find the element when it is searched for" in {
        val result = search.doSearch(44, compare)

        result.index shouldEqual 0
        result.comparisonAtIndex shouldBe Comparison.EQUAL
      }

      "show where an element should belong when not found" in {
        val result1 = search.doSearch(31, compare)

        result1.index shouldEqual 0
        result1.comparisonAtIndex should be (Comparison.LESS_THAN)

        val result2 = search.doSearch(48, compare)

        result2.index shouldEqual 0
        result2.comparisonAtIndex should be (Comparison.GREATER_THAN)
      }
    }

    "doing a search over a list of size n where n is even" should {
      val singleElList = seqAsJavaList(Seq(1, 11, 31, 44))
      val search = new Search[Int](singleElList)

      "find an element at the beginning of the list" in {
        val result = search.doSearch(1, compare)

        result.index shouldEqual 0
        result.comparisonAtIndex shouldBe Comparison.EQUAL
      }

      "find an element at the end of the list" in {
        val result = search.doSearch(44, compare)

        result.index shouldEqual 3
        result.comparisonAtIndex shouldBe Comparison.EQUAL
      }

      "find an element in the middle of the list" in {
        val result = search.doSearch(31, compare)

        result.index shouldEqual 2
        result.comparisonAtIndex shouldBe Comparison.EQUAL
      }

      "show an element belongs at the beginning of a list" in {
        val result = search.doSearch(0, compare)

        result.index shouldEqual 0
        result.comparisonAtIndex shouldBe Comparison.LESS_THAN
      }

      "show an element belongs at the end of a list" in {
        val result = search.doSearch(1000, compare)

        result.index shouldEqual 3
        result.comparisonAtIndex shouldBe Comparison.GREATER_THAN
      }

      "show an element belongs in the middle of a list" in {
        val result = search.doSearch(2, compare)

        result.index shouldEqual 1
        result.comparisonAtIndex shouldBe Comparison.LESS_THAN
      }
    }

    "doing a search over a list of size n where n is odd" should {
      val singleElList = seqAsJavaList(Seq(11, 31, 44))
      val search = new Search[Int](singleElList)

      "find an element at the beginning of the list" in {
        val result = search.doSearch(11, compare)

        result.index shouldEqual 0
        result.comparisonAtIndex shouldBe Comparison.EQUAL
      }

      "find an element at the end of the list" in {
        val result = search.doSearch(44, compare)

        result.index shouldEqual 2
        result.comparisonAtIndex shouldBe Comparison.EQUAL
      }

      "find an element in the middle of the list" in {
        val result = search.doSearch(31, compare)

        result.index shouldEqual 1
        result.comparisonAtIndex shouldBe Comparison.EQUAL
      }

      "show an element belongs at the beginning of a list" in {
        val result = search.doSearch(0, compare)

        result.index shouldEqual 0
        result.comparisonAtIndex shouldBe Comparison.LESS_THAN
      }

      "show an element belongs at the end of a list" in {
        val result = search.doSearch(1000, compare)

        result.index shouldEqual 2
        result.comparisonAtIndex shouldBe Comparison.GREATER_THAN
      }

      "show an element belongs in the middle of a list" in {
        val result = search.doSearch(32, compare)

        result.index shouldEqual 2
        result.comparisonAtIndex shouldBe Comparison.LESS_THAN
      }
    }
  }
}
