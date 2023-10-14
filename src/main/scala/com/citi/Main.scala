package com.citi

import com.citi.job.Job

object Main {
  def main(args: Array[String]): Unit = {
    val job = new Job()
    job.start()
  }
}