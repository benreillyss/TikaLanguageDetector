/*
 * Sample module in the public domain.  Feel free to use this as a template
 * for your modules.
 * 
 *  Contact: Brian Carrier [carrier <at> sleuthkit [dot] org]
 *
 *  This is free and unencumbered software released into the public domain.
 *  
 *  Anyone is free to copy, modify, publish, use, compile, sell, or
 *  distribute this software, either in source code form or as a compiled
 *  binary, for any purpose, commercial or non-commercial, and by any
 *  means.
 *  
 *  In jurisdictions that recognize copyright laws, the author or authors
 *  of this software dedicate any and all copyright interest in the
 *  software to the public domain. We make this dedication for the benefit
 *  of the public at large and to the detriment of our heirs and
 *  successors. We intend this dedication to be an overt act of
 *  relinquishment in perpetuity of all present and future rights to this
 *  software under copyright law.
 *  
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 *  OTHER DEALINGS IN THE SOFTWARE. 
 */
package org.parker.tikalanguagedetector;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import org.sleuthkit.autopsy.coreutils.Logger;
import org.sleuthkit.autopsy.ingest.FileIngestModule;
import org.sleuthkit.autopsy.ingest.IngestModule;
import org.sleuthkit.autopsy.ingest.IngestJobContext;
import org.sleuthkit.autopsy.ingest.IngestMessage;
import org.sleuthkit.autopsy.ingest.IngestServices;
import org.sleuthkit.autopsy.ingest.ModuleDataEvent;
import org.sleuthkit.autopsy.ingest.IngestModuleReferenceCounter;
import org.sleuthkit.datamodel.AbstractFile;
import org.sleuthkit.datamodel.BlackboardArtifact;
import org.sleuthkit.datamodel.BlackboardArtifact.ARTIFACT_TYPE;
import org.sleuthkit.datamodel.BlackboardAttribute;
import org.sleuthkit.datamodel.TskCoreException;
import org.sleuthkit.datamodel.TskData;

import org.apache.tika.exception.TikaException;
import org.apache.tika.langdetect.OptimaizeLangDetector;
//import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.language.*;
import org.openide.util.NbBundle;
import org.sleuthkit.datamodel.ReadContentInputStream;
import org.xml.sax.SAXException;


/**
 * Sample file ingest module that doesn't do much. Demonstrates per ingest job
 * module settings, use of a subset of the available ingest services and
 * thread-safe sharing of per ingest job data.
 */
class TikaLanguageDetectorFileIngestModule implements FileIngestModule {

    private IngestJobContext context = null;
    private static final IngestModuleReferenceCounter refCounter = new IngestModuleReferenceCounter();
    private static final HashMap<Long, Long> artifactCountsForIngestJobs = new HashMap<>();
    private static final BlackboardAttribute.ATTRIBUTE_TYPE LANG_ATTR = BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT_LANGUAGE;

    //static LanguageIdentifier detector;
    static LanguageDetector detector;
    private static HashMap<String, String> langLookup = new HashMap<String,String>();
    
    // Using extensiosn for simple validaiton. This could be improved by using
    // the mime-type definitions, but the File Type identification module does
    // not have to be enabled for an ingestion...
    private static final Set<String> SUPPORTED_EXTENSIONS = new HashSet<>(Arrays.asList(
            new String[] {"doc", "docx", "xls", "xlsx", "ppt", "pptx", "pdf"})); 
    
    @Override
    public void startUp(IngestJobContext context) throws IngestModuleException {
        
        this.context = context;
        refCounter.incrementAndGet(context.getJobId());
        
        // Populate the language lookup map with the human readable form of the 
        // ISO code return value.
        initLookupMap(langLookup);
        
        try {
            detector = new OptimaizeLangDetector().loadModels();
        } catch (IOException ex) {
            // TODO: Update excpetion handling...
            //Exceptions.printStackTrace(ex);
            throw new IngestModule.IngestModuleException(
                    NbBundle.getMessage(TikaLanguageDetectorFileIngestModuleFactory.class, 
                "TikaLanguageDetectorFileIngestModule.languageModelLoadFailure"), ex);
        }
    }

    @Override
    public IngestModule.ProcessResult process(AbstractFile file) {

        // TODO: Add the "Check for canceled job" logic here?

        // Skip anything other than actual file system files.
        if ((file.getType() == TskData.TSK_DB_FILES_TYPE_ENUM.UNALLOC_BLOCKS)
                || (file.getType() == TskData.TSK_DB_FILES_TYPE_ENUM.UNUSED_BLOCKS)
                || (file.isFile() == false)) {
            return IngestModule.ProcessResult.OK;
        }

        // Skip NSRL / known files.
        if (file.getKnown() == TskData.FileKnown.KNOWN) {
            return IngestModule.ProcessResult.OK;
        }

        if (SUPPORTED_EXTENSIONS.contains(file.getNameExtension())){
            // Extracts the text from the file and processes it using Tika's 
            // language detection techniques.
            InputStream in = null;
            BufferedInputStream bin = null;
            try {
                InputStream fileStream= new ReadContentInputStream(file);
                String text = parseExample(fileStream);
                String language = languageDetection(text);
                System.out.println("INFO :: " + file.getName() + " :: " + language);
                
//                // https://www.tutorialspoint.com/tika/tika_language_detection.htm
//                Parser parser = new AutoDetectParser();
//                BodyContentHandler handler = new BodyContentHandler(-1);
//                Metadata metadata = new Metadata();
//                in = new ReadContentInputStream(file);
//                bin = new BufferedInputStream(in);
//                
//                parser.parse(in, handler, metadata, new ParseContext());
                
//                LanguageIdentifier object = new LanguageIdentifier(handler.toString());
//                String language = object.getLanguage();
                System.out.println(file.getName() + " language: " + language);
//                if (object.isReasonablyCertain()){
//                    System.out.println("INFO :: Reasonably Certain");
//                }
                

                // Make an attribute using the ID for the attribute LANG_ATTR 
                // that was previously created.
                BlackboardAttribute attr = new BlackboardAttribute(LANG_ATTR, 
                        TikaLanguageDetectorFileIngestModuleFactory.getModuleName(), 
                        language);

                // Add the to the general info artifact for the file. In a
                // real module, you would likely have more complex data types 
                // and be making more specific artifacts.
                BlackboardArtifact art = file.getGenInfoArtifact();
                art.addAttribute(attr);

                // This method is thread-safe with per ingest job reference counted
                // management of shared data.
                addToBlackboardPostCount(context.getJobId(), 1L);

                // Fire an event to notify any listeners for blackboard postings.
                ModuleDataEvent event = new ModuleDataEvent(
                        TikaLanguageDetectorFileIngestModuleFactory.getModuleName(), 
                        ARTIFACT_TYPE.TSK_GEN_INFO);
                IngestServices.getInstance().fireModuleDataEvent(event);

                return IngestModule.ProcessResult.OK;

            } catch (TskCoreException ex) {
                IngestServices ingestServices = IngestServices.getInstance();
                Logger logger = ingestServices.getLogger(
                        TikaLanguageDetectorFileIngestModuleFactory.getModuleName());
                logger.log(Level.SEVERE, "Error processing file (id = " + file.getId() + ")", ex);
                return IngestModule.ProcessResult.ERROR;
            } catch (IOException | SAXException | TikaException ex) {
                IngestServices ingestServices = IngestServices.getInstance();
                Logger logger = ingestServices.getLogger(
                        TikaLanguageDetectorFileIngestModuleFactory.getModuleName());
                logger.log(Level.SEVERE, "Error processing file (id = " + file.getId() + ")", ex);
                return IngestModule.ProcessResult.ERROR;
            }
        }
        return IngestModule.ProcessResult.OK;
    }

    @Override
    public void shutDown() {
        // This method is thread-safe with per ingest job reference counted
        // management of shared data.
        reportBlackboardPostCount(context.getJobId());
    }

    synchronized static void addToBlackboardPostCount(long ingestJobId, long countToAdd) {
        Long fileCount = artifactCountsForIngestJobs.get(ingestJobId);

        // Ensures that this job has an entry
        if (fileCount == null) {
            fileCount = 0L;
            artifactCountsForIngestJobs.put(ingestJobId, fileCount);
        }

        fileCount += countToAdd;
        artifactCountsForIngestJobs.put(ingestJobId, fileCount);
    }

    synchronized static void reportBlackboardPostCount(long ingestJobId) {
        Long refCount = refCounter.decrementAndGet(ingestJobId);
        if (refCount == 0) {
            Long filesCount = artifactCountsForIngestJobs.remove(ingestJobId);
            String msgText = String.format("Posted %d times to the blackboard", filesCount);
            IngestMessage message = IngestMessage.createMessage(
                    IngestMessage.MessageType.INFO,
                    TikaLanguageDetectorFileIngestModuleFactory.getModuleName(),
                    msgText);
            IngestServices.getInstance().postMessage(message);
        }
    }
    
    // https://tika.apache.org/1.14/examples.html
    public static String parseExample(InputStream inputStream) throws IOException, SAXException, TikaException {
        // setting the BodyContentHandler 'ctor to -1 allows bypasses the init 100000 char setup
        BodyContentHandler handler = new BodyContentHandler(-1);
        AutoDetectParser parser = new AutoDetectParser();
        Metadata metadata = new Metadata();
        try (InputStream stream = inputStream) {
            parser.parse(stream, handler, metadata);
            return handler.toString();
        }
    }
    
    public static String languageDetection(String text) throws IOException {

        LanguageResult result = detector.detect(text);
        detector.reset();
        System.out.println("INFO :: language detected :: " + result.getLanguage());
        
        if (langLookup.containsKey(result.getLanguage())){
            return langLookup.get(result.getLanguage());
        } else {
            return result.getLanguage();
        }
    }
    
    // ref: https://github.com/apache/tika/blob/master/tika-core/src/main/resources/org/apache/tika/language/tika.language.properties
    static void initLookupMap(HashMap<String,String> langLookup){
        langLookup.put("be", "Belarusian");
        langLookup.put("ca", "Catalan");
        langLookup.put("da", "Danish");
        langLookup.put("de", "German");
        langLookup.put("eo", "Esperanto");
        langLookup.put("et", "Estonian");
        langLookup.put("el", "Greek");
        langLookup.put("en", "English");
        langLookup.put("es", "Spanish");
        langLookup.put("fi", "Finnish");
        langLookup.put("fr", "French");
        langLookup.put("fa", "Persian");
        langLookup.put("gl", "Galician");
        langLookup.put("hu", "Hungarian");
        langLookup.put("is", "Icelandic");
        langLookup.put("it", "Italian");
        langLookup.put("lt", "Lithuanian");
        langLookup.put("nl", "Dutch");
        langLookup.put("no", "Norwegian");
        langLookup.put("pl", "Polish");
        langLookup.put("pt", "Portuguese");
        langLookup.put("ro", "Romanian");
        langLookup.put("ru", "Russian");
        langLookup.put("sk", "Slovakian");
        langLookup.put("sl", "Slovenian");
        langLookup.put("sv", "Swedish");
        langLookup.put("th", "Thai");
        langLookup.put("uk", "Ukrainian");
    }
}    