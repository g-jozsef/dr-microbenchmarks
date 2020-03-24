package hu.sztaki.microbenchmark.utils

/**
  * Trait for generating payload for a key
  *
  * @tparam R
  */
trait ValueGenerator[R <: Product] {
  def generateValue(key: Int): R
}

object ValueGenerator {

  // payload type
  type ValueTuple = (Double, String)


  /**
    * Generates constant payload of ValueType
    */
  class ConstantValueGenerator extends ValueGenerator[ValueTuple] {
    override def generateValue(key: Int): (Double, String) =
      (2424.3254543d, "This is a long-long string without any particular meaning")
  }

  /**
    * Abstract class for generating (key, payload) pairs of type (K, R)
    *
    * @param valueGenerator
    * @tparam K
    * @tparam R
    */
  abstract class KeyedRecordGenerator[K, R <: Product](valueGenerator: ValueGenerator[R]) {

    def generateRecord(key: Int): (K, R) = {
      (transformKey(key), valueGenerator.generateValue(key))
    }

    // transforms key to type K
    def transformKey(key: Int): K

    // generate random string as key, but with control over the seed
    def transformKey(k: Int, seed: Int): K

    // transforms key to type K with a postfix
    def transformKey(k: Int, postfix: String): K
  }

  /**
    * Generates string-keyed records with constant payload of ValueType
    */
  class StringKeyedRecordGenerator extends KeyedRecordGenerator[String, ValueTuple](new ConstantValueGenerator) {

    // generate random string as key
    override def transformKey(key: Int): String = StringGenerator.generateString(key)

    // generate random string as key, but with control over the seed
    override def transformKey(k: Int, seed: Int): String = StringGenerator.generateString2(k, seed)

    override def transformKey(k: Int, postfix: String): String = transformKey(k) + "_" + postfix
  }

}