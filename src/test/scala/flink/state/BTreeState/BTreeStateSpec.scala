package flink.state.BTreeState

import flink.state.BTreeState.testutil.MockRuntimeContext

import scala.collection.JavaConversions._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BTreeStateSpec extends AnyWordSpec with Matchers {
  "BTreeState" when {
    "instantiated" should {
      val context = new MockRuntimeContext
      val stateDescriptor = new BTreeStateDescriptor[String, String]("test", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[String]))
      val btree = new BTreeState[String, String](context, stateDescriptor)

      "not be null" in {
        Option(btree) should not be empty
      }

      "return empty values on get" in {
        Option(btree.get("foo")) shouldBe empty
        Option(btree.get("bar")) shouldBe empty
        Option(btree.get("baz")) shouldBe empty
      }
    }

    "values are inserted in reverse order" should {
      val context = new MockRuntimeContext
      val stateDescriptor = new BTreeStateDescriptor[String, String]("test", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[String]))
      val btree = new BTreeState[String, String](context, stateDescriptor)

      btree.insert("3rd", "third")
      btree.insert("2nd", "second")
      btree.insert("1st", "first")

      "return all inserted values in order" in {
        btree.getAllValues.toSeq should contain theSameElementsInOrderAs Seq("first", "second", "third")
      }
    }

    "values are inserted in order" should {
      val context = new MockRuntimeContext
      val stateDescriptor = new BTreeStateDescriptor[String, String]("test", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[String]))
      val btree = new BTreeState[String, String](context, stateDescriptor)

      btree.insert("1st", "first")
      btree.insert("2nd", "second")
      btree.insert("3rd", "third")

      "return all inserted values in order" in {
        btree.getAllValues.toSeq should contain theSameElementsInOrderAs Seq("first", "second", "third")
      }
    }

    "values are inserted in no order" should {
      val context = new MockRuntimeContext
      val stateDescriptor = new BTreeStateDescriptor[String, String]("test", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[String]))
      val btree = new BTreeState[String, String](context, stateDescriptor)

      btree.insert("2nd", "second")
      btree.insert("1st", "first")
      btree.insert("3rd", "third")

      "return all inserted values in order" in {
        btree.getAllValues.toList should contain theSameElementsInOrderAs Seq("first", "second", "third")
      }
    }

    "number of values exceed a single leaf page" should {
      val context = new MockRuntimeContext
      val stateDescriptor = new BTreeStateDescriptor[String, String]("test", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[String]))
      val btree = new BTreeState[String, String](context, stateDescriptor, 64, 64)

      (0 to 128).foreach(n => btree.insert(n.toString, n.toString))

      "return all inserted values in order" in {
        btree.getAllValues.toList shouldBe sorted
      }

      "find a given value in the tree" in {
        btree.get("0") should equal ("0")
        btree.get("111") should equal ("111")
        btree.get("127") should equal ("127")
      }
    }

    "number of values cause root page to split" should {
      val context = new MockRuntimeContext
      val stateDescriptor = new BTreeStateDescriptor[String, String]("test", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[String]))
      val btree = new BTreeState[String, String](context, stateDescriptor, 4, 4)

      (0 to 4 * 4).foreach(n => btree.insert(n.toString, n.toString))

      "return all values in order" in {
        btree.getAllValues.toList shouldBe sorted
      }

      "find a given value in the tree" in {
        btree.get("0") should equal ("0")
        btree.get("14") should equal ("14")
        btree.get("15") should equal ("15")
      }

      "return a range of keys" in {
        btree.getValuesInRange("0", "2").toList should contain theSameElementsInOrderAs
          Seq("0", "1", "10", "11", "12", "13", "14", "15", "16", "2")
      }
    }

    "number of values cause root page to split twice" should {
      val context = new MockRuntimeContext
      val stateDescriptor = new BTreeStateDescriptor[String, String]("test", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[String]))
      val btree = new BTreeState[String, String](context, stateDescriptor, 4, 4)

      (0 to 4 * 4 * 4).foreach(n => btree.insert(n.toString, n.toString))

      "return all values in order" in {
        btree.getAllValues.toList shouldBe sorted
      }

      "find a given value in the tree" in {
        btree.get("0") should equal ("0")
        btree.get("14") should equal ("14")
        btree.get("63") should equal ("63")
      }

      "return a range of keys" in {
        btree.getValuesInRange("0", "2").toList should contain theSameElementsInOrderAs
          Seq("0", "1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "2")
      }
    }

    "a range scan starts at a key that does not exist in the BTree" should {
      val context = new MockRuntimeContext
      val stateDescriptor = new BTreeStateDescriptor[String, String]("test", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[String]))
      val btree = new BTreeState[String, String](context, stateDescriptor, 4, 4)

      (0 to 4 * 4 * 4).foreach(n => btree.insert(n.toString, n.toString))

      "return the keys that do exist in the range" in {
        btree.getValuesInRange(".", "2").toList should contain theSameElementsInOrderAs
          Seq("0", "1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "2")
      }
    }
  }

}
