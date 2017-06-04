package org.logstreaming.analyzer

case class LogRecord(ip: String, time: String, method: String, url: String, httpCode: Integer)
