/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 *  Sample ingest module factory in the public domain.  
 *  Feel free to use this as a template for your inget module factories.
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

import org.openide.util.lookup.ServiceProvider;
import org.openide.util.NbBundle;

import org.sleuthkit.autopsy.ingest.FileIngestModule;
import org.sleuthkit.autopsy.ingest.IngestModuleFactory;
import org.sleuthkit.autopsy.ingest.IngestModuleFactoryAdapter;
import org.sleuthkit.autopsy.ingest.IngestModuleIngestJobSettings;

@ServiceProvider(service = IngestModuleFactory.class) // Sample is discarded at runtime 
public class TikaLanguageDetectorFileIngestModuleFactory extends IngestModuleFactoryAdapter {
    
    private static final String VERSION_NUMBER = "1.0.0";
    
    /**
     * This class method allows the ingest module instances created by this 
     * factory to use the same display name that is provided to the Autopsy
     * ingest framework by the factory.
     *
     */
    static String getModuleName() {
        return NbBundle.getMessage(TikaLanguageDetectorFileIngestModuleFactory.class, 
                "TikaLanguageDetectorFileIngestModuleFactory.moduleName");
    }
    
    /**
     * Gets the display name that identifies the family of ingest modules the
     * factory creates. Autopsy uses this string to identify the module in user
     * interface components and log messages. The module name must be unique. so
     * a brief but distinctive name is recommended.
     *
     * @return The module family display name.
     */
    @Override
    public String getModuleDisplayName() {
        return getModuleName();
    }
    
    /**
     * Gets a brief, user-friendly description of the family of ingest modules
     * the factory creates. Autopsy uses this string to describe the module in
     * user interface components.
     *
     * @return The module family description.
     */
    @Override
    public String getModuleDescription() {
        return NbBundle.getMessage(TikaLanguageDetectorFileIngestModuleFactory.class, 
                "TikaLanguageDetectorFileIngestModuleFactory.moduleDescription");
    }
    
    /**
     * Gets the version number of the family of ingest modules the factory
     * creates.
     *
     * @return The module family version number.
     */
    @Override
    public String getModuleVersionNumber() {
        return VERSION_NUMBER;
    }
    
    /**
     * Queries the factory to determine if it is capable of creating file ingest
     * modules. If the module family does not include file ingest modules, the
     * factory may extend IngestModuleFactoryAdapter to get an implementation of
     * this method that returns false.
     *
     * @return True if the factory can create file ingest modules.
     */
    @Override
    public boolean isFileIngestModuleFactory() {
        return true;
    }
    
    /**
     * Creates a file ingest module instance.
     * <p>
     * Autopsy will generally use the factory to several instances of each type
     * of module for each ingest job it performs. Completing an ingest job
     * entails processing a single data source (e.g., a disk image) and all of
     * the files from the data source, including files extracted from archives
     * and any unallocated space (made to look like a series of files). The data
     * source is passed through one or more pipelines of data source ingest
     * modules. The files are passed through one or more pipelines of file
     * ingest modules.
     * <p>
     * The ingest framework may use multiple threads to complete an ingest job,
     * but it is guaranteed that there will be no more than one module instance
     * per thread. However, if the module instances must share resources, the
     * modules are responsible for synchronizing access to the shared resources
     * and doing reference counting as required to release those resources
     * correctly. Also, more than one ingest job may be in progress at any given
     * time. This must also be taken into consideration when sharing resources
     * between module instances. modules.
     * <p>
     * If the module family does not include file ingest modules, the factory
     * may extend IngestModuleFactoryAdapter to get an implementation of this
     * method that throws an UnsupportedOperationException.
     *
     * @param settings The settings for the ingest job.
     *
     * @return A file ingest module instance.
     */
    @Override
    public FileIngestModule createFileIngestModule(IngestModuleIngestJobSettings ingestOptions) {
        return new TikaLanguageDetectorFileIngestModule();
    }
}
