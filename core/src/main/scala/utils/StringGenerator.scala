package utils

import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
  * Utils to generate random strings
  */
object StringGenerator {
  // use MurmurHash
  val hash: Int => Int =
    (key: Int) => MurmurHash3.stringHash((key.hashCode + 123456791).toString)

  // seeded string hash of a key
  def hash(key: Any, seed: Int): Int = MurmurHash3.stringHash((key.hashCode + seed).toString)

  // length of identifyer
  val idLength: Int = 10
  // alphanumeric characters
  val alphNumList: Seq[Char] = ((48 to 57) ++ (65 to 90) ++ (95 to 95) ++ (97 to 122)).map(_.toChar)

  // generate random alphanumeric id
  def generateId(): String = {
    (1 to idLength).map(_ => {
      val hashed = Random.nextInt(alphNumList.size)
      alphNumList(hashed)
    }).mkString
  }

  // safe modulo
  def %%(dividend: Int, divider: Int) = {
    val mod = dividend % divider
    if (mod < 0) mod + divider else mod
  }

  // convert integer to alphanumeric
  def toAlphNum(hashed: Int): Char =
    alphNumList(%%(hashed, alphNumList.length))

  // random id length but at least 4
  def calculateLength(hashed: Int): Int =
    4 + %%(hashed, 4)

  // hash key to string of varying length
  def generateString(key: Int): String = {
    var part = hash(key)
    val length = calculateLength(part)
    (1 to length).map(_ => {
      part = hash(part)
      toAlphNum(part)
    }).mkString
  }

  // hash key to string of varying length with seed
  def generateString2(key: Int, seed: Int): String = {
    var part = hash(key, seed)
    val length = calculateLength(part)
    (1 to length).map(_ => {
      part = hash(part, seed)
      toAlphNum(part)
    }).mkString
  }

  // generate random string of random length for a key
  def generateRandomString(key: Int): String = {
    var part = Random.nextInt()
    val length = calculateLength(part)
    (1 to length).map(_ => {
      part = Random.nextInt()
      toAlphNum(part)
    }).mkString
  }

  // generate random string of specific length for a key
  def generateRandomStringOfLength(length: Int): String = {
    (1 to length).map(_ => {
      toAlphNum(Random.nextInt())
    }).mkString
  }
}