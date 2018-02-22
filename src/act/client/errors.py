"""
This module defines all exceptions that are used by aCT.
"""
# TODO: check if all exceptions are still used after changes.


class NoSuchJobError(Exception):
    """Error when job is not in database."""

    def __init__(self, jobid):
        """
        Initialize job ID attribute.

        Args:
            jobid: An integer ID of job.
        """
        self.jobid = jobid


class InvalidJobDescriptionError(Exception):
    """Error if given job description is not valid."""
    pass


class NoSuchSiteError(Exception):
    """Error when site is not in configuration."""

    def __init__(self, siteName):
        """
        Initialize site name variable.

        Args:
            siteName: A string with name of site.
        """
        self.siteName = siteName


class InvalidJobRangeError(Exception):
    """Error when job range is invalid."""

    def __init__(self, jobRange):
        """
        Initialize job range attribute.

        Args:
            jobRange: A string that is supposed to be range.
        """
        self.jobRange = jobRange


class InvalidJobIDError(Exception):
    """Error when job ID is not integer."""

    def __init__(self, jobid):
        """
        Initialize job ID attribute.

        Args:
            jobid: A value that is supposed to be ID.
        """
        self.jobid = jobid


class ChangeStateError(Exception):
    """Error when switching between two states is illegal."""

    def __init__(self, fromState, toState):
        """
        Initialize exception attributes.

        Args:
            fromState: The state job was in when user tried to change state.
            toState: The state user wanted to change job's state to.
        """
        self.fromState = fromState
        self.toState = toState


class TmpConfigurationError(Exception):
    """Error when tmp is not configured in aCT configuration."""
    pass


class NoJobDirectoryError(Exception):
    """Error when tmp job results directory does not exist."""

    def __init__(self, jobdir):
        """
        Initialize path attribute.

        Args:
            jobdir: A string with directory path where results should be.
        """
        self.jobdir = jobdir


class JobNotInARCTableWarning(Warning):
    """
    Warning when job is not in arcjobs table.
    
    Happens when job information is needed from ARC table but
    there is no reference to ARC table, because job hasn't
    been submitted to ARC table yet.
    """

    def __init__(self, jobid):
        """
        Initialize job ID attribute.

        Args:
            jobid: An integer ID of job that is not in ARC table yet.
        """
        self.jobid = jobid


class JobNotInARCTableError(Exception):
    """
    Error when job is not in arcjobs table.

    Happens when job reference in ARC table doesn't exist which
    clearly is an error.
    """

    def __init__(self, jobid):
        """
        Initialize job ID attribute.

        Args:
            jobid: An integer ID of job that should be in ARC table.
        """
        self.jobid = jobid


class NoJobDirInARCError(Exception):
    """Error when cannot get job directory name from ARC table."""
    pass


class TargetDirExistsError(Exception):
    """Error when target directory for job already exists."""

    def __init__(self, dstdir):
        """
        Initialize path attribute.

        Args:
            dstdir: A string with existing destination directory path.
        """
        self.dstdir = dstdir


class NoSuchProxyError(Exception):
    """Error when proxy is not found in database."""

    def __init__(self, dn, attribute):
        """
        Initialize proxy attributes.

        Args:
            dn: A string with DN of proxy searched for.
            attribute: A string with proxy attributes of proxy searched for.
        """
        self.dn = dn
        self.attribute = attribute


