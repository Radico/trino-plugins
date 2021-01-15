package com.simondata.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success}

class AesSpec extends AnyWordSpec with Matchers {
  "Aes" when {
    "encrypting" should {
      "produce the same output for the same inputs on multiple runs" in {
        val plainText = "Plugins for Trino".getBytes("utf-8")
        val iv = Aes.randomBytes(16)
        val key = Aes.randomBytes(16)

        val cipherTextA = Aes.encrypt(plainText, key, Some(iv)).get
        val cipherTextB = Aes.encrypt(plainText, key, Some(iv)).get

        assert(cipherTextA.sameElements(cipherTextB))
      }

      "produce different output for the same input, same IV, and different keys" in {
        val plainText = "Plugins for Trino".getBytes("utf-8")
        val iv = Aes.randomBytes(16)
        val keyA = Aes.randomBytes(16)
        val keyB = Aes.randomBytes(16)

        val cipherTextA = Aes.encrypt(plainText, keyA, Some(iv)).get
        val cipherTextB = Aes.encrypt(plainText, keyB, Some(iv)).get

        assert(!cipherTextA.sameElements(cipherTextB))
      }

      "produce different output for the same input, same key, and different IV" in {
        val plainText = "Plugins for Trino".getBytes("utf-8")
        val ivA = Aes.randomBytes(16)
        val ivB = Aes.randomBytes(16)
        val key = Aes.randomBytes(16)

        val cipherTextA = Aes.encrypt(plainText, key, Some(ivA)).get
        val cipherTextB = Aes.encrypt(plainText, key, Some(ivB)).get

        assert(!cipherTextA.sameElements(cipherTextB))
      }
    }

    "decrypting" should {
      "succeed for the correct IV and key" in {
        val plainText = "Plugins for Trino".getBytes("utf-8")
        val iv = Aes.randomBytes(16)
        val key = Aes.randomBytes(16)
        val cipherText = Aes.encrypt(plainText, key, Some(iv)).get

        val recoveredText = Aes.decrypt(cipherText, key, Some(iv)).get

        assert(plainText.sameElements(recoveredText))
      }

      "produce the same output for the same inputs on multiple runs" in {
        val plainText = "Plugins for Trino".getBytes("utf-8")
        val iv = Aes.randomBytes(16)
        val key = Aes.randomBytes(16)
        val cipherText = Aes.encrypt(plainText, key, Some(iv)).get

        val plainTextA = Aes.decrypt(cipherText, key, Some(iv)).get
        val plainTextB = Aes.decrypt(cipherText, key, Some(iv)).get

        assert(plainTextA.sameElements(plainTextB))
      }

      "fail for the wrong key" in {
        val plainText = "Plugins for Trino".getBytes("utf-8")
        val iv = Aes.randomBytes(16)
        val keyA = Aes.randomBytes(16)
        val keyB = Aes.randomBytes(16)
        val cipherText = Aes.encrypt(plainText, keyA, Some(iv)).get

        val failed = Aes.decrypt(cipherText, keyB, Some(iv)) match {
          case Failure(_) => true
          case Success(recoveredText) => !recoveredText.sameElements(plainText)
        }

        assert(failed)
      }

      "fail for the wrong IV" in {
        val plainText = "Plugins for Trino".getBytes("utf-8")
        val ivA = Aes.randomBytes(16)
        val ivB = Aes.randomBytes(16)
        val key = Aes.randomBytes(16)
        val cipherText = Aes.encrypt(plainText, key, Some(ivA)).get

        val failed = Aes.decrypt(cipherText, key, Some(ivB)) match {
          case Failure(_) => true
          case Success(recoveredText) => !recoveredText.sameElements(plainText)
        }

        assert(failed)
      }
    }
  }
}
