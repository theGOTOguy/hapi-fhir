package ca.uhn.fhir.jpa.bulk.imprt.provider;

/*-
 * #%L
 * HAPI FHIR JPA Server
 * %%
 * Copyright (C) 2014 - 2021 Smile CDR, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.bulk.imprt.api.IBulkDataImportSvc;
import ca.uhn.fhir.jpa.bulk.imprt.model.BulkImportJobFileJson;
import ca.uhn.fhir.jpa.bulk.imprt.model.BulkImportJobJson;
import ca.uhn.fhir.jpa.bulk.imprt.model.JobFileRowProcessingModeEnum;
import ca.uhn.fhir.jpa.model.util.JpaConstants;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.api.Constants;
import ca.uhn.fhir.rest.api.PreferHeader;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.RestfulServerUtils;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import ca.uhn.fhir.util.OperationOutcomeUtil;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.fileupload.MultipartStream;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HeaderElement;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeaderValueParser;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.hl7.fhir.r4.model.InstantType;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletResponse;

import static org.slf4j.LoggerFactory.getLogger;


public class BulkDataImportProvider {
	private static final Logger ourLog = getLogger(BulkDataImportProvider.class);
        private final int DEFAULT_BUFSIZE = 1024;

	@Autowired
	private IBulkDataImportSvc myBulkDataImportSvc;
	@Autowired
	private FhirContext myFhirContext;

	@VisibleForTesting
	public void setFhirContextForUnitTest(FhirContext theFhirContext) {
		myFhirContext = theFhirContext;
	}

	@VisibleForTesting
	public void setBulkDataImportSvcForUnitTests(IBulkDataImportSvc theBulkDataImportSvc) {
		myBulkDataImportSvc = theBulkDataImportSvc;
	}

	/**
	 * $import
	 */
	@Operation(name = JpaConstants.OPERATION_IMPORT, global = false /* set to true once we can handle this */, manualResponse = true, idempotent = true)
	public void imprt(
		@OperationParam(name = JpaConstants.PARAM_IMPORT_JOB_DESCRIPTION, min = 0, max = 1, typeName = "string") IPrimitiveType<String> theJobDescription,
		@OperationParam(name = JpaConstants.PARAM_IMPORT_PROCESSING_MODE, min = 0, max = 1, typeName = "string") IPrimitiveType<String> theProcessingMode,
		@OperationParam(name = JpaConstants.PARAM_IMPORT_FILE_COUNT, min = 0, max = 1, typeName = "integer") IPrimitiveType<Integer> theFileCount,
		@OperationParam(name = JpaConstants.PARAM_IMPORT_BATCH_SIZE, min = 0, max = 1, typeName = "integer") IPrimitiveType<Integer> theBatchSize,
		ServletRequestDetails theRequestDetails
	) {
                validatePreferAsyncHeader(theRequestDetails);
                BulkImportJobJson theImportJobJson = new BulkImportJobJson();

                if (theJobDescription != null) {
                        theImportJobJson.setJobDescription(theJobDescription.getValueAsString());
                }

                theImportJobJson.setProcessingMode(theProcessingMode == null ? JobFileRowProcessingModeEnum.FHIR_TRANSACTION : JobFileRowProcessingModeEnum.valueOf(theProcessingMode.getValueAsString()));
                theImportJobJson.setBatchSize(theBatchSize == null ? 1 : theBatchSize.getValue());

                List<BulkImportJobFileJson> theInitialFiles = new ArrayList<BulkImportJobFileJson>();
                // TODO:  Remove these notes.
                // Multipart Files: https://stackoverflow.com/questions/8659808/how-does-http-file-upload-work
                //                  https://medium.com/@petehouston/upload-files-with-curl-93064dcccc76                

                // The request type must be multipart/form-data
                String contentTypeHeader = theRequestDetails.getHeader("Content-Type");
                HeaderElement headerElement = BasicHeaderValueParser.parseHeaderElement(contentTypeHeader, null);

                // Parse out the boundary element.
                if (headerElement.getValue() == "multipart/form-data") {
                        NameValuePair boundary = headerElement.getParameterByName("boundary");

                        if (boundary != null) {
                                try {
                                        String boundaryValue = boundary.getValue();
                                        ReaderInputStream theInputStream = new ReaderInputStream(theRequestDetails.getReader(), theRequestDetails.getCharset());
                                        MultipartStream theMultipartStream = new MultipartStream(theInputStream, boundaryValue.getBytes(theRequestDetails.getCharset()), boundaryValue.length() + 5 > DEFAULT_BUFSIZE ? boundaryValue.length() + 5 : DEFAULT_BUFSIZE , null);

                                        boolean nextPart = theMultipartStream.skipPreamble();
                                        while (nextPart) {
                                                BulkImportJobFileJson theJobFile = new BulkImportJobFileJson();
                                        
                                                theJobFile.setTenantName(theRequestDetails.getTenantId());
                                                theJobFile.setDescription(theMultipartStream.readHeaders());

                                                ByteArrayOutputStream theOutputStream = new ByteArrayOutputStream();
                                                theMultipartStream.readBodyData(theOutputStream);
                                                // TODO:  In a later version of Java, theOutputStream.toString()
                                                // accepts theRequestDetails.getCharset().
                                                theJobFile.setContents(theOutputStream.toString());

                                                theInitialFiles.add(theJobFile);
                                                nextPart = theMultipartStream.readBoundary();
                                        }
                                } catch (IOException e) {
                                        // If something went wrong, it was almost certainly an issue with the request.
                                        throw new InvalidRequestException(e.getMessage());
                                }
                        }
                } else {
                        throw new InvalidRequestException("Content-Type must be multipart/form-data for $import.");
                }

                ourLog.info("Done parsing...");

                // If theFileCount is null, let's substitute the count from the multipart stream.
                theImportJobJson.setFileCount(theFileCount == null ? theInitialFiles.size() : theFileCount.getValue());

                // Start the job.  
                String theJob = myBulkDataImportSvc.createNewJob(theImportJobJson, theInitialFiles);
                myBulkDataImportSvc.markJobAsReadyForActivation(theJob);

		writePollingLocationToResponseHeaders(theRequestDetails, theJob);
	}

        /**
         * $import-poll-status
         */
        @Operation(name = JpaConstants.OPERATION_IMPORT_POLL_STATUS, manualResponse = true, idempotent = true)
        public void importPollStatus(
                @OperationParam(name = JpaConstants.PARAM_IMPORT_POLL_STATUS_JOB_ID, typeName = "string", min = 0, max = 1) IPrimitiveType<String> theJobId,
                ServletRequestDetails theRequestDetails
        ) throws IOException {

                HttpServletResponse response = theRequestDetails.getServletResponse();
                theRequestDetails.getServer().addHeadersToResponse(response);

                IBulkDataImportSvc.JobInfo status = myBulkDataImportSvc.getJobStatus(theJobId.getValueAsString());
                IBaseOperationOutcome oo;

                switch (status.getStatus()) {
                        case STAGING:
                        case READY:
                        case RUNNING:
                                response.setStatus(Constants.STATUS_HTTP_202_ACCEPTED);
                                response.addHeader(Constants.HEADER_X_PROGRESS, "Status set to " + status.getStatus() + " at " + new InstantType(status.getStatusTime()).getValueAsString());
                                response.addHeader(Constants.HEADER_RETRY_AFTER, "120");
                                break;
                        case COMPLETE:
                                response.setStatus(Constants.STATUS_HTTP_200_OK);
                                response.setContentType(Constants.CT_FHIR_JSON);

                                // Create an OperationOutcome response
                                oo = OperationOutcomeUtil.newInstance(myFhirContext);
                                myFhirContext.newJsonParser().setPrettyPrint(true).encodeResourceToWriter(oo, response.getWriter());
                                response.getWriter().close();
                                break;
                        case ERROR:
                                response.setStatus(Constants.STATUS_HTTP_500_INTERNAL_ERROR);
                                response.setContentType(Constants.CT_FHIR_JSON);

                                // Create an OperationOutcome response
                                oo = OperationOutcomeUtil.newInstance(myFhirContext);
                                OperationOutcomeUtil.addIssue(myFhirContext, oo, "error", status.getStatusMessage(), null, null);
                                myFhirContext.newJsonParser().setPrettyPrint(true).encodeResourceToWriter(oo, response.getWriter());
                                response.getWriter().close();
                }
        }

	public void writePollingLocationToResponseHeaders(ServletRequestDetails theRequestDetails, String theJob) {
		String serverBase = getServerBase(theRequestDetails);
		String pollLocation = serverBase + "/" + JpaConstants.OPERATION_IMPORT_POLL_STATUS + "?" + JpaConstants.PARAM_IMPORT_POLL_STATUS_JOB_ID + "=" + theJob;

		HttpServletResponse response = theRequestDetails.getServletResponse();

		// Add standard headers
		theRequestDetails.getServer().addHeadersToResponse(response);

		// Successful 202 Accepted
		response.addHeader(Constants.HEADER_CONTENT_LOCATION, pollLocation);
		response.setStatus(Constants.STATUS_HTTP_202_ACCEPTED);
	}

        private String getServerBase(ServletRequestDetails theRequestDetails) {
                return StringUtils.removeEnd(theRequestDetails.getServerBaseForRequest(), "/");
        }

	private void validatePreferAsyncHeader(ServletRequestDetails theRequestDetails) {
		String preferHeader = theRequestDetails.getHeader(Constants.HEADER_PREFER);
		PreferHeader prefer = RestfulServerUtils.parsePreferHeader(null, preferHeader);
		if (prefer.getRespondAsync() == false) {
			throw new InvalidRequestException("Must request async processing for $import");
		}
	}
}
