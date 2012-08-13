package org.openflamingo.hadoop.util;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

/**
 * XML 문서를 들여쓰기와 문서의 폭을 기준으로 포맷팅하는 유틸리티.
 *
 * @author Edward KIM
 */
public class XmlFormatter {

	/**
	 * 포맷팅되지 않은 XML 문자열을 포맷팅한다. 포맷팅 기준은
	 * 들여쓰기 4, 폭은 80 문자이다.
	 *
	 * @param unformattedXml 포맷팅 되지 않은 XML 문자열
	 * @return 포맷팅한 XML
	 */
	public static String format(String unformattedXml) {
		try {
			Document document = parseXmlFile(unformattedXml);
			OutputFormat format = new OutputFormat(document);
			format.setLineWidth(80);
			format.setIndenting(true);
			format.setIndent(4);
			Writer out = new StringWriter();
			XMLSerializer serializer = new XMLSerializer(out, format);
			serializer.serialize(document);
			return out.toString();
		} catch (Exception e) {
			return unformattedXml;
		}
	}

	/**
	 * XML 문자열을 파싱한다.
	 *
	 * @param xml XML 문자열
	 * @return Document
	 */
	private static Document parseXmlFile(String xml) {
		try {
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			InputSource inputSource = new InputSource(new StringReader(xml));
			return documentBuilder.parse(inputSource);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
