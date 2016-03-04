/**
 * 
 */
package org.athento.nuxeo.operations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.athento.nuxeo.workers.DeleteVersionsWorker;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.automation.core.collectors.DocumentModelCollector;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.impl.blob.InputStreamBlob;
import org.nuxeo.ecm.core.api.security.ACE;
import org.nuxeo.ecm.core.api.security.ACL;
import org.nuxeo.ecm.core.api.security.ACP;
import org.nuxeo.ecm.core.api.security.impl.ACLImpl;
import org.nuxeo.ecm.core.api.security.impl.ACPImpl;
import org.nuxeo.ecm.core.work.api.Work.State;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.runtime.api.Framework;


/**
 * @author athento
 *
 */

@Operation(id = DeleteVersionsWorkerOperation.ID, category = "Athento", 
label = "Delete versions (as worker)", description = "Launch a worker to Delete Versions")
public class DeleteVersionsWorkerOperation {

	public static final String ID = "Athento.DeleteVersions_as_worker";

	@Context
	protected CoreSession session;

	@Param(name = "conditions")
	protected String conditions;

	@Param(name = "simulate", required = false, values = "true")
	boolean simulate = true;

	@OperationMethod
	public String run() throws Exception {
		launchWorker();
		return "OK";
	}

	private void launchWorker () {
		if (_log.isInfoEnabled()) {
			_log.info("Launching worker...");
		}
		DeleteVersionsWorker work = new DeleteVersionsWorker(conditions, simulate);
		WorkManager workManager = Framework.getLocalService(WorkManager.class);
		workManager.schedule(work, WorkManager.Scheduling.IF_NOT_RUNNING_OR_SCHEDULED);
		String workId = work.getId();
		State workState = workManager.getWorkState(workId);
		if (_log.isInfoEnabled()) {
			_log.info("Work [" + workId + "] queued in state [" + workState + "]");
		}
	}
	private static final Log _log = LogFactory.getLog(DeleteVersionsWorkerOperation.class);

}