/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.JAXBCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.display.DisplayData;

import com.google.common.base.Preconditions;

import org.codehaus.stax2.XMLInputFactory2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventHandler;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

// CHECKSTYLE.OFF: JavadocStyle
/**
 * A source that can be used to read XML files. This source reads one or more
 * XML files and creates a {@code PCollection} of a given type. An Dataflow read transform can be
 * created by passing an {@code XmlSource} object to {@code Read.from()}. Please note the
 * example given below.
 *
 * <p>The XML file must be of the following form, where {@code root} and {@code record} are XML
 * element names that are defined by the user:
 *
 * <pre>
 * {@code
 * <root>
 * <record> ... </record>
 * <record> ... </record>
 * <record> ... </record>
 * ...
 * <record> ... </record>
 * </root>
 * }
 * </pre>
 *
 * <p>Basically, the XML document should contain a single root element with an inner list consisting
 * entirely of record elements. The records may contain arbitrary XML content; however, that content
 * <b>must not</b> contain the start {@code <record>} or end {@code </record>} tags. This
 * restriction enables reading from large XML files in parallel from different offsets in the file.
 *
 * <p>Root and/or record elements may additionally contain an arbitrary number of XML attributes.
 * Additionally users must provide a class of a JAXB annotated Java type that can be used convert
 * records into Java objects and vice versa using JAXB marshalling/unmarshalling mechanisms. Reading
 * the source will generate a {@code PCollection} of the given JAXB annotated Java type.
 * Optionally users may provide a minimum size of a bundle that should be created for the source.
 *
 * <p>The following example shows how to read from {@link XmlSource} in a Dataflow pipeline:
 *
 * <pre>
 * {@code
 * XmlSource<String> source = XmlSource.<String>from(file.toPath().toString())
 *     .withRootElement("root")
 *     .withRecordElement("record")
 *     .withRecordClass(Record.class);
 * PCollection<String> output = p.apply(Read.from(source));
 * }
 * </pre>
 *
 * <p>Currently, only XML files that use single-byte characters are supported. Using a file that
 * contains multi-byte characters may result in data loss or duplication.
 *
 * <p>To use {@link XmlSource}:
 * <ol>
 *   <li>Explicitly declare a dependency on org.codehaus.woodstox:stax2-api</li>
 *   <li>Include a compatible implementation on the classpath at run-time,
 *       such as org.codehaus.woodstox:woodstox-core-asl</li>
 * </ol>
 *
 * <p>These dependencies have been declared as optional in Maven sdk/pom.xml file of
 * Google Cloud Dataflow.
 *
 * <p><h3>Permissions</h3>
 * Permission requirements depend on the
 * {@link org.apache.beam.sdk.runners.PipelineRunner PipelineRunner} that is
 * used to execute the Dataflow job. Please refer to the documentation of corresponding
 * {@link PipelineRunner PipelineRunners} for more details.
 *
 * @param <T> Type of the objects that represent the records of the XML file. The
 *        {@code PCollection} generated by this source will be of this type.
 */
// CHECKSTYLE.ON: JavadocStyle
public class XmlSource<T> extends FileBasedSource<T> {

  private static final String XML_VERSION = "1.1";
  private static final int DEFAULT_MIN_BUNDLE_SIZE = 8 * 1024;
  private final String rootElement;
  private final String recordElement;
  private final Class<T> recordClass;

  /**
   * Creates an XmlSource for a single XML file or a set of XML files defined by a Java "glob" file
   * pattern. Each XML file should be of the form defined in {@link XmlSource}.
   */
  public static <T> XmlSource<T> from(String fileOrPatternSpec) {
    return new XmlSource<>(fileOrPatternSpec, DEFAULT_MIN_BUNDLE_SIZE, null, null, null);
  }

  /**
   * Sets name of the root element of the XML document. This will be used to create a valid starting
   * root element when initiating a bundle of records created from an XML document. This is a
   * required parameter.
   */
  public XmlSource<T> withRootElement(String rootElement) {
    return new XmlSource<>(
        getFileOrPatternSpec(), getMinBundleSize(), rootElement, recordElement, recordClass);
  }

  /**
   * Sets name of the record element of the XML document. This will be used to determine offset of
   * the first record of a bundle created from the XML document. This is a required parameter.
   */
  public XmlSource<T> withRecordElement(String recordElement) {
    return new XmlSource<>(
        getFileOrPatternSpec(), getMinBundleSize(), rootElement, recordElement, recordClass);
  }

  /**
   * Sets a JAXB annotated class that can be populated using a record of the provided XML file. This
   * will be used when unmarshalling record objects from the XML file.  This is a required
   * parameter.
   */
  public XmlSource<T> withRecordClass(Class<T> recordClass) {
    return new XmlSource<>(
        getFileOrPatternSpec(), getMinBundleSize(), rootElement, recordElement, recordClass);
  }

  /**
   * Sets a parameter {@code minBundleSize} for the minimum bundle size of the source. Please refer
   * to {@link OffsetBasedSource} for the definition of minBundleSize.  This is an optional
   * parameter.
   */
  public XmlSource<T> withMinBundleSize(long minBundleSize) {
    return new XmlSource<>(
        getFileOrPatternSpec(), minBundleSize, rootElement, recordElement, recordClass);
  }

  private XmlSource(String fileOrPattern, long minBundleSize, String rootElement,
      String recordElement, Class<T> recordClass) {
    super(fileOrPattern, minBundleSize);
    this.rootElement = rootElement;
    this.recordElement = recordElement;
    this.recordClass = recordClass;
  }

  private XmlSource(String fileOrPattern, long minBundleSize, long startOffset, long endOffset,
      String rootElement, String recordElement, Class<T> recordClass) {
    super(fileOrPattern, minBundleSize, startOffset, endOffset);
    this.rootElement = rootElement;
    this.recordElement = recordElement;
    this.recordClass = recordClass;
  }

  @Override
  protected FileBasedSource<T> createForSubrangeOfFile(String fileName, long start, long end) {
    return new XmlSource<T>(
        fileName, getMinBundleSize(), start, end, rootElement, recordElement, recordClass);
  }

  @Override
  protected FileBasedReader<T> createSingleFileReader(PipelineOptions options) {
    return new XMLReader<T>(this);
  }

  @Override
  public boolean producesSortedKeys(PipelineOptions options) throws Exception {
    return false;
  }

  @Override
  public void validate() {
    super.validate();
    Preconditions.checkNotNull(
        rootElement, "rootElement is null. Use builder method withRootElement() to set this.");
    Preconditions.checkNotNull(
        recordElement,
        "recordElement is null. Use builder method withRecordElement() to set this.");
    Preconditions.checkNotNull(
        recordClass, "recordClass is null. Use builder method withRecordClass() to set this.");
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {

    builder
        .add("filePattern", getFileOrPatternSpec())
        .addIfNotNull("rootElement", rootElement)
        .addIfNotNull("recordElement", recordElement)
        .addIfNotNull("recordClass", recordClass);

    long minBundleSize = getMinBundleSize();
    builder.addIfNotDefault("minBundleSize", minBundleSize, DEFAULT_MIN_BUNDLE_SIZE);
  }

  @Override
  public Coder<T> getDefaultOutputCoder() {
    return JAXBCoder.of(recordClass);
  }

  public String getRootElement() {
    return rootElement;
  }

  public String getRecordElement() {
    return recordElement;
  }

  public Class<T> getRecordClass() {
    return recordClass;
  }

  /**
   * A {@link Source.Reader} for reading JAXB annotated Java objects from an XML file. The XML
   * file should be of the form defined at {@link XmlSource}.
   *
   * <p>Timestamped values are currently unsupported - all values implicitly have the timestamp
   * of {@code BoundedWindow.TIMESTAMP_MIN_VALUE}.
   *
   * @param <T> Type of objects that will be read by the reader.
   */
  private static class XMLReader<T> extends FileBasedReader<T> {
    // The amount of bytes read from the channel to memory when determining the starting offset of
    // the first record in a bundle. After matching to starting offset of the first record the
    // remaining bytes read to this buffer and the bytes still not read from the channel are used to
    // create the XML parser.
    private static final int BUF_SIZE = 1024;

    // This should be the maximum number of bytes a character will encode to, for any encoding
    // supported by XmlSource. Currently this is set to 4 since UTF-8 characters may be
    // four bytes.
    private static final int MAX_CHAR_BYTES = 4;

    // In order to support reading starting in the middle of an XML file, we construct an imaginary
    // well-formed document (a header and root tag followed by the contents of the input starting at
    // the record boundary) and feed it to the parser. Because of this, the offset reported by the
    // XML parser is not the same as offset in the original file. They differ by a constant amount:
    // offsetInOriginalFile = parser.getLocation().getCharacterOffset() + parserBaseOffset;
    // Note that this is true only for files with single-byte characters.
    // It appears that, as of writing, there does not exist a Java XML parser capable of correctly
    // reporting byte offsets of elements in the presence of multi-byte characters.
    private long parserBaseOffset = 0;
    private boolean readingStarted = false;

    // If true, the current bundle does not contain any records.
    private boolean emptyBundle = false;

    private Unmarshaller jaxbUnmarshaller = null;
    private XMLStreamReader parser = null;

    private T currentRecord = null;

    // Byte offset of the current record in the XML file provided when creating the source.
    private long currentByteOffset = 0;

    public XMLReader(XmlSource<T> source) {
      super(source);

      // Set up a JAXB Unmarshaller that can be used to unmarshall record objects.
      try {
        JAXBContext jaxbContext = JAXBContext.newInstance(getCurrentSource().recordClass);
        jaxbUnmarshaller = jaxbContext.createUnmarshaller();

        // Throw errors if validation fails. JAXB by default ignores validation errors.
        jaxbUnmarshaller.setEventHandler(new ValidationEventHandler() {
          @Override
          public boolean handleEvent(ValidationEvent event) {
            throw new RuntimeException(event.getMessage(), event.getLinkedException());
          }
        });
      } catch (JAXBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public synchronized XmlSource<T> getCurrentSource() {
      return (XmlSource<T>) super.getCurrentSource();
    }

    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
      // This method determines the correct starting offset of the first record by reading bytes
      // from the ReadableByteChannel. This implementation does not need the channel to be a
      // SeekableByteChannel.
      // The method tries to determine the first record element in the byte channel. The first
      // record must start with the characters "<recordElement" where "recordElement" is the
      // record element of the XML document described above. For the match to be complete this
      // has to be followed by one of following.
      // * any whitespace character
      // * '>' character
      // * '/' character (to support empty records).
      //
      // After this match this method creates the XML parser for parsing the XML document,
      // feeding it a fake document consisting of an XML header and the <rootElement> tag followed
      // by the contents of channel starting from <recordElement. The <rootElement> tag may be never
      // closed.

      // This stores any bytes that should be used prior to the remaining bytes of the channel when
      // creating an XML parser object.
      ByteArrayOutputStream preambleByteBuffer = new ByteArrayOutputStream();
      // A dummy declaration and root for the document with proper XML version and encoding. Without
      // this XML parsing may fail or may produce incorrect results.

      byte[] dummyStartDocumentBytes =
          ("<?xml version=\"" + XML_VERSION + "\" encoding=\"UTF-8\" ?>"
              + "<" + getCurrentSource().rootElement + ">").getBytes(StandardCharsets.UTF_8);
      preambleByteBuffer.write(dummyStartDocumentBytes);
      // Gets the byte offset (in the input file) of the first record in ReadableByteChannel. This
      // method returns the offset and stores any bytes that should be used when creating the XML
      // parser in preambleByteBuffer.
      long offsetInFileOfRecordElement =
          getFirstOccurenceOfRecordElement(channel, preambleByteBuffer);
      if (offsetInFileOfRecordElement < 0) {
        // Bundle has no records. So marking this bundle as an empty bundle.
        emptyBundle = true;
        return;
      } else {
        byte[] preambleBytes = preambleByteBuffer.toByteArray();
        currentByteOffset = offsetInFileOfRecordElement;
        setUpXMLParser(channel, preambleBytes);
        parserBaseOffset = offsetInFileOfRecordElement - dummyStartDocumentBytes.length;
      }
      readingStarted = true;
    }

    // Gets the first occurrence of the next record within the given ReadableByteChannel. Puts
    // any bytes read past the starting offset of the next record back to the preambleByteBuffer.
    // If a record is found, returns the starting offset of the record, otherwise
    // returns -1.
    private long getFirstOccurenceOfRecordElement(
        ReadableByteChannel channel, ByteArrayOutputStream preambleByteBuffer) throws IOException {
      int byteIndexInRecordElementToMatch = 0;
      // Index of the byte in the string "<recordElement" to be matched
      // against the current byte from the stream.
      boolean recordStartBytesMatched = false; // "<recordElement" matched. Still have to match the
      // next character to confirm if this is a positive match.
      boolean fullyMatched = false; // If true, record element was fully matched.

      // This gives the offset of the byte currently being read. We do a '-1' here since we
      // increment this value at the beginning of the while loop below.
      long offsetInFileOfCurrentByte = getCurrentSource().getStartOffset() - 1;
      long startingOffsetInFileOfCurrentMatch = -1;
      // If this is non-negative, currently there is a match in progress and this value gives the
      // starting offset of the match currently being conducted.
      boolean matchStarted = false; // If true, a match is currently in progress.

      // These two values are used to determine the character immediately following a match for
      // "<recordElement". Please see the comment for 'MAX_CHAR_BYTES' above.
      byte[] charBytes = new byte[MAX_CHAR_BYTES];
      int charBytesFound = 0;

      ByteBuffer buf = ByteBuffer.allocate(BUF_SIZE);
      byte[] recordStartBytes =
          ("<" + getCurrentSource().recordElement).getBytes(StandardCharsets.UTF_8);

      outer: while (channel.read(buf) > 0) {
        buf.flip();
        while (buf.hasRemaining()) {
          offsetInFileOfCurrentByte++;
          byte b = buf.get();
          boolean reset = false;
          if (recordStartBytesMatched) {
            // We already matched "<recordElement" reading the next character to determine if this
            // is a positive match for a new record.
            charBytes[charBytesFound] = b;
            charBytesFound++;
            Character c = null;
            if (charBytesFound == charBytes.length) {
              CharBuffer charBuf = CharBuffer.allocate(1);
              InputStream charBufStream = new ByteArrayInputStream(charBytes);
              java.io.Reader reader =
                  new InputStreamReader(charBufStream, StandardCharsets.UTF_8);
              int read = reader.read();
              if (read <= 0) {
                return -1;
              }
              charBuf.flip();
              c = (char) read;
            } else {
              continue;
            }

            // Record start may be of following forms
            // * "<recordElement<whitespace>..."
            // * "<recordElement>..."
            // * "<recordElement/..."
            if (Character.isWhitespace(c) || c == '>' || c == '/') {
              fullyMatched = true;
              // Add the recordStartBytes and charBytes to preambleByteBuffer since these were
              // already read from the channel.
              preambleByteBuffer.write(recordStartBytes);
              preambleByteBuffer.write(charBytes);
              // Also add the rest of the current buffer to preambleByteBuffer.
              while (buf.hasRemaining()) {
                preambleByteBuffer.write(buf.get());
              }
              break outer;
            } else {
              // Matching was unsuccessful. Reset the buffer to include bytes read for the char.
              ByteBuffer newbuf = ByteBuffer.allocate(BUF_SIZE);
              newbuf.put(charBytes);
              offsetInFileOfCurrentByte -= charBytes.length;
              while (buf.hasRemaining()) {
                newbuf.put(buf.get());
              }
              newbuf.flip();
              buf = newbuf;

              // Ignore everything and try again starting from the current buffer.
              reset = true;
            }
          } else if (b == recordStartBytes[byteIndexInRecordElementToMatch]) {
            // Next byte matched.
            if (!matchStarted) {
              // Match was for the first byte, record the starting offset.
              matchStarted = true;
              startingOffsetInFileOfCurrentMatch = offsetInFileOfCurrentByte;
            }
            byteIndexInRecordElementToMatch++;
          } else {
            // Not a match. Ignore everything and try again starting at current point.
            reset = true;
          }
          if (reset) {
            // Clear variables and try to match starting from the next byte.
            byteIndexInRecordElementToMatch = 0;
            startingOffsetInFileOfCurrentMatch = -1;
            matchStarted = false;
            recordStartBytesMatched = false;
            charBytes = new byte[MAX_CHAR_BYTES];
            charBytesFound = 0;
          }
          if (byteIndexInRecordElementToMatch == recordStartBytes.length) {
            // "<recordElement" matched. Need to still check next byte since this might be an
            // element that has "recordElement" as a prefix.
            recordStartBytesMatched = true;
          }
        }
        buf.clear();
      }

      if (!fullyMatched) {
        return -1;
      } else {
        return startingOffsetInFileOfCurrentMatch;
      }
    }

    private void setUpXMLParser(ReadableByteChannel channel, byte[] lookAhead) throws IOException {
      try {
        // We use Woodstox because the StAX implementation provided by OpenJDK reports
        // character locations incorrectly. Note that Woodstox still currently reports *byte*
        // locations incorrectly when parsing documents that contain multi-byte characters.
        XMLInputFactory2 xmlInputFactory = (XMLInputFactory2) XMLInputFactory.newInstance();
        this.parser = xmlInputFactory.createXMLStreamReader(
            new SequenceInputStream(
                new ByteArrayInputStream(lookAhead), Channels.newInputStream(channel)),
            "UTF-8");

        // Current offset should be the offset before reading the record element.
        while (true) {
          int event = parser.next();
          if (event == XMLStreamConstants.START_ELEMENT) {
            String localName = parser.getLocalName();
            if (localName.equals(getCurrentSource().recordElement)) {
              break;
            }
          }
        }
      } catch (FactoryConfigurationError | XMLStreamException e) {
        throw new IOException(e);
      }
    }

    @Override
    protected boolean readNextRecord() throws IOException {
      if (emptyBundle) {
        currentByteOffset = Long.MAX_VALUE;
        return false;
      }
      try {
        // Update current offset and check if the next value is the record element.
        currentByteOffset = parserBaseOffset + parser.getLocation().getCharacterOffset();
        while (parser.getEventType() != XMLStreamConstants.START_ELEMENT) {
          parser.next();
          currentByteOffset = parserBaseOffset + parser.getLocation().getCharacterOffset();
          if (parser.getEventType() == XMLStreamConstants.END_DOCUMENT) {
            currentByteOffset = Long.MAX_VALUE;
            return false;
          }
        }
        JAXBElement<T> jb = jaxbUnmarshaller.unmarshal(parser, getCurrentSource().recordClass);
        currentRecord = jb.getValue();
        return true;
      } catch (JAXBException | XMLStreamException e) {
        throw new IOException(e);
      }
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (!readingStarted) {
        throw new NoSuchElementException();
      }
      return currentRecord;
    }

    @Override
    protected boolean isAtSplitPoint() {
      // Every record is at a split point.
      return true;
    }

    @Override
    protected long getCurrentOffset() {
      return currentByteOffset;
    }
  }
}
