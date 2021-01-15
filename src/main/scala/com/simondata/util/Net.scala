package com.simondata.util

import java.net.{InetAddress, NetworkInterface}

import scala.jdk.CollectionConverters._

object Net {
  def interfaces: List[NetworkInterface] = NetworkInterface
    .networkInterfaces()
    .iterator
    .asScala
    .filter(!_.isVirtual)
    .filter(!_.isLoopback)
    .filter(_.isUp)
    .foldLeft[List[NetworkInterface]](Nil) { case (list, interface) => interface :: list }

  def addresses: List[InetAddress] = interfaces flatMap { _.getInetAddresses.asScala.toList }

  /**
   * Fetch a list of candidate external IP addresses.
   *
   * These are addresses attached to non-virtual, non-loopback interfaces which are up.
   * In addition, the IP addresses are non-local and non-multicast.
   *
   * @return a list of IPs
   */
  def ips: List[String] = addresses
    .filter(!_.isAnyLocalAddress)
    .filter(!_.isLinkLocalAddress)
    .filter(!_.isLoopbackAddress)
    .filter(!_.isMulticastAddress)
    .map(_.getHostAddress)
}
