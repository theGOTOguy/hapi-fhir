package ca.uhn.fhir.mdm.provider;

/*-
 * #%L
 * HAPI FHIR - Master Data Management
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
import ca.uhn.fhir.mdm.api.IMdmControllerSvc;
import ca.uhn.fhir.mdm.api.IMdmMatchFinderSvc;
import ca.uhn.fhir.mdm.api.IMdmSettings;
import ca.uhn.fhir.mdm.api.IMdmSubmitSvc;
import ca.uhn.fhir.mdm.api.MatchedTarget;
import ca.uhn.fhir.mdm.api.MdmConstants;
import ca.uhn.fhir.mdm.api.MdmLinkJson;
import ca.uhn.fhir.mdm.api.paging.MdmPageRequest;
import ca.uhn.fhir.mdm.model.MdmTransactionContext;
import ca.uhn.fhir.model.api.annotation.Description;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.api.Constants;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.provider.ProviderConstants;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import ca.uhn.fhir.util.BundleBuilder;
import ca.uhn.fhir.util.ParametersUtil;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.instance.model.api.IAnyResource;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseBackboneElement;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseDatatype;
import org.hl7.fhir.instance.model.api.IBaseExtension;
import org.hl7.fhir.instance.model.api.IBaseParameters;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.slf4j.Logger;
import org.springframework.data.domain.Page;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static ca.uhn.fhir.rest.api.Constants.PARAM_OFFSET;
import static org.slf4j.LoggerFactory.getLogger;

public class MdmProviderDstu3Plus extends BaseMdmProvider {
	private static final Logger ourLog = getLogger(MdmProviderDstu3Plus.class);

	private final IMdmControllerSvc myMdmControllerSvc;
	private final IMdmMatchFinderSvc myMdmMatchFinderSvc;
	private final IMdmSubmitSvc myMdmSubmitSvc;
	private final IMdmSettings myMdmSettings;

	public static final int DEFAULT_PAGE_SIZE = 20;
	public static final int MAX_PAGE_SIZE = 100;

	/**
	 * Constructor
	 * <p>
	 * Note that this is not a spring bean. Any necessary injections should
	 * happen in the constructor
	 */
	public MdmProviderDstu3Plus(FhirContext theFhirContext, IMdmControllerSvc theMdmControllerSvc, IMdmMatchFinderSvc theMdmMatchFinderSvc, IMdmSubmitSvc theMdmSubmitSvc, IMdmSettings theIMdmSettings) {
		super(theFhirContext);
		myMdmControllerSvc = theMdmControllerSvc;
		myMdmMatchFinderSvc = theMdmMatchFinderSvc;
		myMdmSubmitSvc = theMdmSubmitSvc;
		myMdmSettings = theIMdmSettings;
	}

	@Operation(name = ProviderConstants.EMPI_MATCH, typeName = "Patient")
	public IBaseBundle match(@OperationParam(name = ProviderConstants.MDM_MATCH_RESOURCE, min = 1, max = 1, typeName = "Patient") IAnyResource thePatient) {
		if (thePatient == null) {
			throw new InvalidRequestException("resource may not be null");
		}
		return getMatchesAndPossibleMatchesForResource(thePatient, "Patient");
	}

	@Operation(name = ProviderConstants.MDM_MATCH)
	public IBaseBundle serverMatch(@OperationParam(name = ProviderConstants.MDM_MATCH_RESOURCE, min = 1, max = 1) IAnyResource theResource,
											 @OperationParam(name = ProviderConstants.MDM_RESOURCE_TYPE, min = 1, max = 1, typeName = "string") IPrimitiveType<String> theResourceType
	) {
		if (theResource == null) {
			throw new InvalidRequestException("resource may not be null");
		}
		return getMatchesAndPossibleMatchesForResource(theResource, theResourceType.getValueAsString());
	}

	/**
	 * Helper method which will return a bundle of all Matches and Possible Matches.
	 */
	private IBaseBundle getMatchesAndPossibleMatchesForResource(IAnyResource theResource, String theResourceType) {
		List<MatchedTarget> matches = myMdmMatchFinderSvc.getMatchedTargets(theResourceType, theResource);
		matches.sort(Comparator.comparing((MatchedTarget m) -> m.getMatchResult().getNormalizedScore()).reversed());

		BundleBuilder builder = new BundleBuilder(myFhirContext);
		builder.setBundleField("type", "searchset");
		builder.setBundleField("id", UUID.randomUUID().toString());
		builder.setMetaField("lastUpdated", builder.newPrimitive("instant", new Date()));

		IBaseBundle retVal = builder.getBundle();
		for (MatchedTarget next : matches) {
			boolean shouldKeepThisEntry = next.isMatch() || next.isPossibleMatch();
			if (!shouldKeepThisEntry) {
				continue;
			}

			IBase entry = builder.addEntry();
			builder.addToEntry(entry, "resource", next.getTarget());

			IBaseBackboneElement search = builder.addSearch(entry);
			toBundleEntrySearchComponent(builder, search, next);
		}
		return retVal;
	}


	public IBaseBackboneElement toBundleEntrySearchComponent(BundleBuilder theBuilder, IBaseBackboneElement theSearch, MatchedTarget theMatchedTarget) {
		theBuilder.setSearchField(theSearch, "mode", "match");
		double score = theMatchedTarget.getMatchResult().getNormalizedScore();
		theBuilder.setSearchField(theSearch, "score",
			theBuilder.newPrimitive("decimal", BigDecimal.valueOf(score)));

		String matchGrade = getMatchGrade(theMatchedTarget);
		IBaseDatatype codeType = (IBaseDatatype) myFhirContext.getElementDefinition("code").newInstance(matchGrade);
		IBaseExtension searchExtension = theSearch.addExtension();
		searchExtension.setUrl(MdmConstants.FIHR_STRUCTURE_DEF_MATCH_GRADE_URL_NAMESPACE);
		searchExtension.setValue(codeType);

		return theSearch;
	}

	@Operation(name = ProviderConstants.MDM_MERGE_GOLDEN_RESOURCES)
	public IBaseResource mergeGoldenResources(@OperationParam(name = ProviderConstants.MDM_MERGE_GR_FROM_GOLDEN_RESOURCE_ID, min = 1, max = 1, typeName = "string") IPrimitiveType<String> theFromGoldenResourceId,
															@OperationParam(name = ProviderConstants.MDM_MERGE_GR_TO_GOLDEN_RESOURCE_ID, min = 1, max = 1, typeName = "string") IPrimitiveType<String> theToGoldenResourceId,
															@OperationParam(name = ProviderConstants.MDM_MERGE_RESOURCE, max = 1) IAnyResource theMergedResource,
															RequestDetails theRequestDetails) {
		validateMergeParameters(theFromGoldenResourceId, theToGoldenResourceId);

		MdmTransactionContext.OperationType operationType = (theMergedResource == null) ?
			MdmTransactionContext.OperationType.MERGE_GOLDEN_RESOURCES : MdmTransactionContext.OperationType.MANUAL_MERGE_GOLDEN_RESOURCES;
		MdmTransactionContext txContext = createMdmContext(theRequestDetails, operationType,
			getResourceType(ProviderConstants.MDM_MERGE_GR_FROM_GOLDEN_RESOURCE_ID, theFromGoldenResourceId));
		return myMdmControllerSvc.mergeGoldenResources(theFromGoldenResourceId.getValueAsString(), theToGoldenResourceId.getValueAsString(), theMergedResource, txContext);
	}

	@Operation(name = ProviderConstants.MDM_UPDATE_LINK)
	public IBaseResource updateLink(@OperationParam(name = ProviderConstants.MDM_UPDATE_LINK_GOLDEN_RESOURCE_ID, min = 1, max = 1) IPrimitiveType<String> theGoldenResourceId,
											  @OperationParam(name = ProviderConstants.MDM_UPDATE_LINK_RESOURCE_ID, min = 1, max = 1) IPrimitiveType<String> theResourceId,
											  @OperationParam(name = ProviderConstants.MDM_UPDATE_LINK_MATCH_RESULT, min = 1, max = 1) IPrimitiveType<String> theMatchResult,
											  ServletRequestDetails theRequestDetails) {
		validateUpdateLinkParameters(theGoldenResourceId, theResourceId, theMatchResult);
		return myMdmControllerSvc.updateLink(theGoldenResourceId.getValueAsString(), theResourceId.getValue(),
			theMatchResult.getValue(), createMdmContext(theRequestDetails, MdmTransactionContext.OperationType.UPDATE_LINK,
				getResourceType(ProviderConstants.MDM_UPDATE_LINK_GOLDEN_RESOURCE_ID, theGoldenResourceId))
		);
	}

	@Operation(name = ProviderConstants.OPERATION_MDM_CLEAR, returnParameters = {
		@OperationParam(name = ProviderConstants.OPERATION_BATCH_RESPONSE_JOB_ID, typeName = "decimal")
	})
	public IBaseParameters clearMdmLinks(@OperationParam(name = ProviderConstants.OPERATION_MDM_CLEAR_RESOURCE_NAME, min = 0, max = OperationParam.MAX_UNLIMITED, typeName = "string") List<IPrimitiveType<String>> theResourceNames,
													 @OperationParam(name = ProviderConstants.OPERATION_MDM_CLEAR_BATCH_SIZE, typeName = "decimal", min = 0, max = 1) IPrimitiveType<BigDecimal> theBatchSize,
													 ServletRequestDetails theRequestDetails) {

		List<String> resourceNames = new ArrayList<>();


		if (theResourceNames != null) {
			resourceNames.addAll(theResourceNames.stream().map(IPrimitiveType::getValue).collect(Collectors.toList()));
			validateResourceNames(resourceNames);
		} else {
			resourceNames.addAll(myMdmSettings.getMdmRules().getMdmTypes());
		}

		List<String> urls = resourceNames.stream().map(s -> s + "?").collect(Collectors.toList());
		return myMdmControllerSvc.submitMdmClearJob(urls, theBatchSize, theRequestDetails);
	}

	private void validateResourceNames(List<String> theResourceNames) {
		for (String resourceName : theResourceNames) {
			if (!myMdmSettings.isSupportedMdmType(resourceName)) {
				throw new InvalidRequestException(ProviderConstants.OPERATION_MDM_CLEAR + " does not support resource type: " + resourceName);
			}
		}
	}

	@Operation(name = ProviderConstants.MDM_QUERY_LINKS, idempotent = true)
	public IBaseParameters queryLinks(@OperationParam(name = ProviderConstants.MDM_QUERY_LINKS_GOLDEN_RESOURCE_ID, min = 0, max = 1, typeName = "string") IPrimitiveType<String> theGoldenResourceId,
												 @OperationParam(name = ProviderConstants.MDM_QUERY_LINKS_RESOURCE_ID, min = 0, max = 1, typeName = "string") IPrimitiveType<String> theResourceId,
												 @OperationParam(name = ProviderConstants.MDM_QUERY_LINKS_MATCH_RESULT, min = 0, max = 1, typeName = "string") IPrimitiveType<String> theMatchResult,
												 @OperationParam(name = ProviderConstants.MDM_QUERY_LINKS_LINK_SOURCE, min = 0, max = 1, typeName = "string")
													 IPrimitiveType<String> theLinkSource,

												 @Description(formalDefinition = "Results from this method are returned across multiple pages. This parameter controls the offset when fetching a page.")
												 @OperationParam(name = PARAM_OFFSET, min = 0, max = 1, typeName = "integer")
														 IPrimitiveType<Integer> theOffset,
												 @Description(formalDefinition = "Results from this method are returned across multiple pages. This parameter controls the size of those pages.")
												 @OperationParam(name = Constants.PARAM_COUNT, min = 0, max = 1, typeName = "integer")
														 IPrimitiveType<Integer> theCount,

												 ServletRequestDetails theRequestDetails) {
		MdmPageRequest mdmPageRequest = new MdmPageRequest(theOffset, theCount, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE);
		Page<MdmLinkJson> mdmLinkJson = myMdmControllerSvc.queryLinks(extractStringOrNull(theGoldenResourceId),
			extractStringOrNull(theResourceId), extractStringOrNull(theMatchResult), extractStringOrNull(theLinkSource),
			createMdmContext(theRequestDetails, MdmTransactionContext.OperationType.QUERY_LINKS,
				getResourceType(ProviderConstants.MDM_QUERY_LINKS_GOLDEN_RESOURCE_ID, theGoldenResourceId)), mdmPageRequest);

		return parametersFromMdmLinks(mdmLinkJson, true, theRequestDetails, mdmPageRequest);
	}

	@Operation(name = ProviderConstants.MDM_DUPLICATE_GOLDEN_RESOURCES, idempotent = true)
	public IBaseParameters getDuplicateGoldenResources(
		@Description(formalDefinition="Results from this method are returned across multiple pages. This parameter controls the offset when fetching a page.")
		@OperationParam(name = PARAM_OFFSET, min = 0, max = 1, typeName = "integer")
			IPrimitiveType<Integer> theOffset,
		@Description(formalDefinition = "Results from this method are returned across multiple pages. This parameter controls the size of those pages.")
		@OperationParam(name = Constants.PARAM_COUNT, min = 0, max = 1, typeName = "integer")
			IPrimitiveType<Integer> theCount,
		ServletRequestDetails theRequestDetails) {

		MdmPageRequest mdmPageRequest = new MdmPageRequest(theOffset, theCount, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE);

		Page<MdmLinkJson> possibleDuplicates = myMdmControllerSvc.getDuplicateGoldenResources(createMdmContext(theRequestDetails, MdmTransactionContext.OperationType.DUPLICATE_GOLDEN_RESOURCES, null), mdmPageRequest);

		return parametersFromMdmLinks(possibleDuplicates, false, theRequestDetails, mdmPageRequest);
	}

	@Operation(name = ProviderConstants.MDM_NOT_DUPLICATE)
	public IBaseParameters notDuplicate(@OperationParam(name = ProviderConstants.MDM_QUERY_LINKS_GOLDEN_RESOURCE_ID, min = 1, max = 1, typeName = "string") IPrimitiveType<String> theGoldenResourceId,
													@OperationParam(name = ProviderConstants.MDM_QUERY_LINKS_RESOURCE_ID, min = 1, max = 1, typeName = "string") IPrimitiveType<String> theResourceId,
													ServletRequestDetails theRequestDetails) {

		validateNotDuplicateParameters(theGoldenResourceId, theResourceId);
		myMdmControllerSvc.notDuplicateGoldenResource(theGoldenResourceId.getValue(), theResourceId.getValue(),
			createMdmContext(theRequestDetails, MdmTransactionContext.OperationType.NOT_DUPLICATE,
				getResourceType(ProviderConstants.MDM_QUERY_LINKS_GOLDEN_RESOURCE_ID, theGoldenResourceId))
		);

		IBaseParameters retval = ParametersUtil.newInstance(myFhirContext);
		ParametersUtil.addParameterToParametersBoolean(myFhirContext, retval, "success", true);
		return retval;
	}

	@Operation(name = ProviderConstants.OPERATION_MDM_SUBMIT, idempotent = false, returnParameters = {
		@OperationParam(name = ProviderConstants.OPERATION_BATCH_RESPONSE_JOB_ID, typeName = "integer")
	})
	public IBaseParameters mdmBatchOnAllSourceResources(
		@OperationParam(name = ProviderConstants.MDM_BATCH_RUN_RESOURCE_TYPE, min = 0, max = 1, typeName = "string") IPrimitiveType<String> theResourceType,
		@OperationParam(name = ProviderConstants.MDM_BATCH_RUN_CRITERIA, min = 0, max = 1, typeName = "string") IPrimitiveType<String> theCriteria,
		ServletRequestDetails theRequestDetails) {
		String criteria = convertStringTypeToString(theCriteria);
		String resourceType = convertStringTypeToString(theResourceType);
		long submittedCount;
		if (resourceType != null) {
			submittedCount = myMdmSubmitSvc.submitSourceResourceTypeToMdm(resourceType, criteria);
		} else {
			submittedCount = myMdmSubmitSvc.submitAllSourceTypesToMdm(criteria);
		}
		return buildMdmOutParametersWithCount(submittedCount);
	}

	private String convertStringTypeToString(IPrimitiveType<String> theCriteria) {
		return theCriteria == null ? null : theCriteria.getValueAsString();
	}


	@Operation(name = ProviderConstants.OPERATION_MDM_SUBMIT, idempotent = false, typeName = "Patient", returnParameters = {
		@OperationParam(name = ProviderConstants.OPERATION_BATCH_RESPONSE_JOB_ID, typeName = "integer")
	})
	public IBaseParameters mdmBatchPatientInstance(
		@IdParam IIdType theIdParam,
		RequestDetails theRequest) {
		long submittedCount = myMdmSubmitSvc.submitSourceResourceToMdm(theIdParam);
		return buildMdmOutParametersWithCount(submittedCount);
	}

	@Operation(name = ProviderConstants.OPERATION_MDM_SUBMIT, idempotent = false, typeName = "Patient", returnParameters = {
		@OperationParam(name = ProviderConstants.OPERATION_BATCH_RESPONSE_JOB_ID, typeName = "integer")
	})
	public IBaseParameters mdmBatchPatientType(
		@OperationParam(name = ProviderConstants.MDM_BATCH_RUN_CRITERIA, typeName = "string") IPrimitiveType<String> theCriteria,
		RequestDetails theRequest) {
		String criteria = convertStringTypeToString(theCriteria);
		long submittedCount = myMdmSubmitSvc.submitPatientTypeToMdm(criteria);
		return buildMdmOutParametersWithCount(submittedCount);
	}

	@Operation(name = ProviderConstants.OPERATION_MDM_SUBMIT, idempotent = false, typeName = "Practitioner", returnParameters = {
		@OperationParam(name = ProviderConstants.OPERATION_BATCH_RESPONSE_JOB_ID, typeName = "integer")
	})
	public IBaseParameters mdmBatchPractitionerInstance(
		@IdParam IIdType theIdParam,
		RequestDetails theRequest) {
		long submittedCount = myMdmSubmitSvc.submitSourceResourceToMdm(theIdParam);
		return buildMdmOutParametersWithCount(submittedCount);
	}

	@Operation(name = ProviderConstants.OPERATION_MDM_SUBMIT, idempotent = false, typeName = "Practitioner", returnParameters = {
		@OperationParam(name = ProviderConstants.OPERATION_BATCH_RESPONSE_JOB_ID, typeName = "integer")
	})
	public IBaseParameters mdmBatchPractitionerType(
		@OperationParam(name = ProviderConstants.MDM_BATCH_RUN_CRITERIA, typeName = "string") IPrimitiveType<String> theCriteria,
		RequestDetails theRequest) {
		String criteria = convertStringTypeToString(theCriteria);
		long submittedCount = myMdmSubmitSvc.submitPractitionerTypeToMdm(criteria);
		return buildMdmOutParametersWithCount(submittedCount);
	}

	/**
	 * Helper function to build the out-parameters for all batch MDM operations.
	 */
	public IBaseParameters buildMdmOutParametersWithCount(long theCount) {
		IBaseParameters retval = ParametersUtil.newInstance(myFhirContext);
		ParametersUtil.addParameterToParametersLong(myFhirContext, retval, ProviderConstants.OPERATION_BATCH_RESPONSE_JOB_ID, theCount);
		return retval;
	}

	@Nonnull
	protected String getMatchGrade(MatchedTarget theTheMatchedTarget) {
		String retVal = "probable";
		if (theTheMatchedTarget.isMatch()) {
			retVal = "certain";
		} else if (theTheMatchedTarget.isPossibleMatch()) {
			retVal = "possible";
		}
		return retVal;
	}

	private String getResourceType(String theParamName, IPrimitiveType<String> theResourceId) {
		if (theResourceId != null) {
			return getResourceType(theParamName, theResourceId.getValueAsString());
		} else {
			return MdmConstants.UNKNOWN_MDM_TYPES;
		}
	}

	private String getResourceType(String theParamName, String theResourceId) {
		if (StringUtils.isEmpty(theResourceId)) {
			return MdmConstants.UNKNOWN_MDM_TYPES;
		}
		IIdType idType = MdmControllerUtil.getGoldenIdDtOrThrowException(theParamName, theResourceId);
		return idType.getResourceType();
	}
}
