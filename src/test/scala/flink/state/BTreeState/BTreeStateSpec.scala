package flink.state.BTreeState

import scala.collection.JavaConversions._

import flink.state.BTreeState.util.MockRuntimeContext
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

    "values are inserted" should {
      val context = new MockRuntimeContext
      val stateDescriptor = new BTreeStateDescriptor[String, String]("test", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[String]))
      val btree = new BTreeState[String, String](context, stateDescriptor)

      btree.insert("1st", "first")
      btree.insert("3rd", "third")
      btree.insert("2nd", "second")

      "values should be in order" in {
        btree.getAllValues.toSeq should contain theSameElementsInOrderAs Seq("first", "second", "third")
      }
    }
  }

}
