package utils

import scala.util.Random
import scala.util.hashing.MurmurHash3

object StringGenerator {
  val hash: Int => Int =
    (key: Int) => MurmurHash3.stringHash((key.hashCode + 123456791).toString)

  def hash(key: Any, seed: Int) = MurmurHash3.stringHash((key.hashCode + seed).toString)

  val idLength: Int = 10
  val alphNumList = ((48 to 57) ++ (65 to 90) ++ (95 to 95) ++ (97 to 122)).map(_.toChar)

  def generateId(): String = {
    (1 to idLength).map(_ => {
      val hashed = Random.nextInt(alphNumList.size)
      alphNumList(hashed)
    }).mkString
  }

  def %%(dividend: Int, divider: Int) = {
    val mod = dividend % divider
    if (mod < 0) mod + divider else mod
  }

  def toAlphNum(hashed: Int): Char =
    alphNumList(%%(hashed, alphNumList.length))

  def calculateLength(hashed: Int): Int =
    4 + %%(hashed, 4)

  def generateString(key: Int): String = {
    var part = hash(key)
    val length = calculateLength(part)
    (1 to length).map(_ => {
      part = hash(part)
      toAlphNum(part)
    }).mkString
  }

  def generateString2(key: Int, seed: Int): String = {
    var part = hash(key, seed)
    val length = calculateLength(part)
    (1 to length).map(_ => {
      part = hash(part, seed)
      toAlphNum(part)
    }).mkString
  }

  def generateRandomString(key: Int): String = {
    var part = Random.nextInt()
    val length = calculateLength(part)
    (1 to length).map(_ => {
      part = Random.nextInt()
      toAlphNum(part)
    }).mkString
  }

  def generateRandomStringOfLength(length: Int): String = {
    (1 to length).map(_ => {
      toAlphNum(Random.nextInt())
    }).mkString
  }
}