/**
 * 
 */
package org.athento.nuxeo.workers;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.text.Document;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.IdRef;
import org.nuxeo.ecm.core.api.IterableQueryResult;
import org.nuxeo.ecm.core.api.security.ACP;
import org.nuxeo.ecm.core.query.sql.NXQL;
import org.nuxeo.ecm.core.work.AbstractWork;

/**
 * @author athento
 *
 */
public class DeleteVersionsWorker extends AbstractWork {

	/**
	 * For version 1.0
	 */
	private static final long serialVersionUID = 8077878313385599097L;

	public DeleteVersionsWorker(String id) {
		super(id);
	}

	public DeleteVersionsWorker(String _conditions, boolean _simulate) {
		conditions = _conditions;
		simulate = _simulate;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.nuxeo.ecm.core.work.api.Work#getTitle()
	 */
	@Override
	public String getTitle() {
		// TODO Auto-generated method stub
		return getCategory() + " " + conditions;
	}

	@Override
	public String getCategory() {
		return CATEGORY;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.nuxeo.ecm.core.work.AbstractWork#work()
	 */
	@Override
	public void work() throws Exception {
		if (_log.isInfoEnabled()) {
			_log.info("=== " + (simulate ? "SIMULATING" : "INITIALIZING")
					+ " Work with query (" + conditions + ")");
		}
		List<DocumentModel> result = null;
		initSession();
		setProgress(new Progress(0));
		try {
			setStatus("Fetching results");
			String NX_QUERY = DeleteVersionsWorker.QUERY_CHECKED_IN
					+ (conditions != null && !conditions.isEmpty() ? " AND "
							+ conditions : "");
			if (_log.isInfoEnabled()) {
				_log.info("Query for checked out documents: " + NX_QUERY);
			}

			long startTime = System.currentTimeMillis();
			result = session.query(NX_QUERY);
			long stopTime = System.currentTimeMillis();
			long elapsedTime = stopTime - startTime;
			String msg = "#" + result.size() + " results fetched in "
					+ elapsedTime + " ms";
			setStatus(msg);
			if (_log.isInfoEnabled()) {
				_log.info(msg);
			}
			setProgress(new Progress(33));
			if (result.size() > 0) {
				setStatus("Checking out #" + result.size() + " documents");
				for (DocumentModel document : result) {
					boolean isCheckedOut = document.isCheckedOut();
					if (_log.isInfoEnabled()) {
						_log.info("Document [" + document.getId() + "|"
								+ document.getPathAsString()
								+ "] is checked out: " + isCheckedOut);
					}
					if (!isCheckedOut) {
						if (_log.isInfoEnabled()) {
							_log.info(" checking out document ["
									+ document.getId() + "|"
									+ document.getPathAsString() + "]");
						}
						if (!simulate) {
							try {
								document.checkOut();
								if (_log.isInfoEnabled()) {
									_log.info(" [ok] document ["
											+ document.getId() + "|"
											+ document.getPathAsString()
											+ "] checked out");
								}
							} catch (Exception e) {
								_log.error(" !! Unable to checkout document ["
										+ document.getPathAsString() + "]: "
										+ e.getMessage(), e);
							}
						}
					}
				}
				setStatus("Saving checked out documents");
				if (!simulate) {
					commitOrRollbackTransaction();
					startTransaction();
				}
			} else {
				if (_log.isInfoEnabled()) {
					_log.info(" 0 results found!");
				}
			}
			setProgress(new Progress(50));
			setStatus("Searching versions");
			NX_QUERY = DeleteVersionsWorker.QUERY_VERSIONS
					+ (conditions != null && !conditions.isEmpty() ? " AND "
							+ conditions : "");
			if (_log.isInfoEnabled()) {
				_log.info("Query for deleting versions: " + NX_QUERY);
			}
			startTime = System.currentTimeMillis();
			result = session.query(NX_QUERY);
			stopTime = System.currentTimeMillis();
			elapsedTime = stopTime - startTime;
			msg = "#" + result.size() + " results fetched in " + elapsedTime
					+ " ms";
			setStatus(msg);
			if (_log.isInfoEnabled()) {
				_log.info(msg);
			}
			setProgress(new Progress(66));
			if (result.size() > 0) {
				setStatus("Deleting #" + result.size() + " documents");
				for (DocumentModel document : result) {
					if (_log.isInfoEnabled()) {
						_log.info(" deleting document [" + document.getId()
								+ "|" + document.getPathAsString() + "]");
					}
					if (!simulate) {
						try {
							session.removeDocument(document.getRef());
							if (_log.isInfoEnabled()) {
								_log.info(" [ok] document deleted ["
										+ document.getId() + "|"
										+ document.getPathAsString() + "]");
							}
						} catch (Exception e) {
							_log.error(
									" !! Unable to delete document ["
											+ document.getPathAsString()
											+ "]: " + e.getMessage(), e);
						}
					}
				}
			} else {
				if (_log.isInfoEnabled()) {
					_log.info(" 0 results found!");
				}
			}
		} finally {
			if (!simulate) {
				commitOrRollbackTransaction();
				startTransaction();
			}
			setProgress(new Progress(100));
		}
		if (_log.isInfoEnabled()) {
			_log.info("=== ENDING Work for query (" + conditions + ")");
		}
	}

	private String conditions;
	private boolean simulate;

	private static final String QUERY_CHECKED_IN = "SELECT * FROM Document WHERE "
			+ " ecm:currentLifeCycleState != 'deleted' AND "
			+ " ecm:isVersion = 0 AND "
			+ " ecm:mixinType != 'HiddenInNavigation' "
			+ " AND "
			+ " ecm:mixinType != 'Folderish' AND ecm:isCheckedIn = 1 ";
	private static final String QUERY_VERSIONS = "SELECT * FROM Document WHERE "
			+ " ecm:currentLifeCycleState != 'deleted' AND "
			+ " ecm:isVersion = 1 AND "
			+ " ecm:mixinType != 'HiddenInNavigation' "
			+ " AND "
			+ " ecm:mixinType != 'Folderish' ";
	private static final String CATEGORY = "DeleteVersions ";
	private static final Log _log = LogFactory
			.getLog(DeleteVersionsWorker.class);

}
