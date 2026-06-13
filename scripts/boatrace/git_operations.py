"""Git commit and push operations."""

import subprocess
from pathlib import Path
from typing import List, Optional
from . import logger as logging_module


class GitError(Exception):
    """Git operation failed."""

    pass


def get_git_config(key: str) -> Optional[str]:
    """Get git configuration value.

    Args:
        key: Configuration key (e.g., user.name)

    Returns:
        Configuration value or None if not set
    """
    try:
        result = subprocess.run(
            ["git", "config", key],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception as e:
        logging_module.warning(
            "git_config_read_failed",
            key=key,
            error=str(e),
        )
        return None


def set_git_config(key: str, value: str, local: bool = True) -> bool:
    """Set git configuration value.

    Args:
        key: Configuration key
        value: Configuration value
        local: Set locally (True) or globally (False)

    Returns:
        True if successful, False otherwise
    """
    try:
        args = ["git", "config", key, value]
        if local:
            args.insert(2, "--local")

        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode == 0:
            logging_module.debug(
                "git_config_set",
                key=key,
                local=local,
            )
            return True
        else:
            logging_module.warning(
                "git_config_set_failed",
                key=key,
                error=result.stderr,
            )
            return False

    except Exception as e:
        logging_module.warning(
            "git_config_set_error",
            key=key,
            error=str(e),
        )
        return False


def stage_files(files: List[str]) -> bool:
    """Stage files for commit.

    Args:
        files: List of file paths to stage

    Returns:
        True if successful, False otherwise
    """
    if not files:
        return True

    try:
        # Get the project root (parent of scripts directory)
        # This ensures git commands work correctly regardless of where the script is run from
        project_root = Path(__file__).parent.parent.parent
        
        subprocess.run(
            ["git", "add"] + files,
            capture_output=True,
            check=True,
            cwd=str(project_root),
        )

        logging_module.debug(
            "files_staged",
            files_count=len(files),
        )
        return True

    except subprocess.CalledProcessError as e:
        logging_module.error(
            "stage_files_failed",
            files_count=len(files),
            error=e.stderr.decode() if e.stderr else str(e),
        )
        return False

    except Exception as e:
        logging_module.error(
            "stage_files_error",
            error=str(e),
        )
        return False


def commit(message: str) -> Optional[str]:
    """Create git commit.

    Args:
        message: Commit message

    Returns:
        Commit hash if successful, None otherwise
    """
    try:
        # Get the project root
        project_root = Path(__file__).parent.parent.parent
        
        result = subprocess.run(
            ["git", "commit", "-m", message],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(project_root),
        )

        if result.returncode == 0:
            # Extract commit hash from output
            # Output format: [branch hash] message...
            output = result.stdout
            commit_hash = None

            if "[" in output and "]" in output:
                start = output.find("[") + 1
                end = output.find("]")
                commit_info = output[start:end].strip()
                if " " in commit_info:
                    commit_hash = commit_info.split()[-1]

            logging_module.info(
                "commit_created",
                commit=commit_hash or "unknown",
                message=message,
            )
            return commit_hash

        elif "nothing to commit" in result.stdout:
            logging_module.info(
                "commit_skipped",
                reason="no_changes",
            )
            return None

        else:
            logging_module.error(
                "commit_failed",
                error=result.stdout or result.stderr,
            )
            return None

    except Exception as e:
        logging_module.error(
            "commit_error",
            error=str(e),
        )
        return None


def push(branch: str = "main", force: bool = False) -> bool:
    """Push commits to remote.

    Args:
        branch: Branch name to push
        force: Force push (use with caution)

    Returns:
        True if successful, False otherwise
    """
    try:
        # Get the project root
        project_root = Path(__file__).parent.parent.parent
        
        # Fetch latest remote changes
        fetch_result = subprocess.run(
            ["git", "fetch", "origin", branch],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(project_root),
        )
        
        # Rebase local commits on top of remote
        rebase_result = subprocess.run(
            ["git", "rebase", f"origin/{branch}"],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(project_root),
        )
        
        if rebase_result.returncode != 0:
            logging_module.error(
                "rebase_failed",
                branch=branch,
                error=rebase_result.stderr,
            )
            return False
        
        args = ["git", "push", "origin", branch]
        if force:
            args.append("-f")

        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=False,
            cwd=str(project_root),
        )

        if result.returncode == 0:
            logging_module.info(
                "push_success",
                branch=branch,
            )
            return True

        else:
            # Check for specific errors
            error_output = result.stdout + result.stderr
            logging_module.error(
                "push_failed_detail",
                branch=branch,
                stdout=result.stdout,
                stderr=result.stderr,
                returncode=result.returncode,
            )
            if "authentication" in error_output.lower():
                logging_module.error(
                    "push_auth_failed",
                    branch=branch,
                    error="Authentication failed",
                )
            elif "rejected" in error_output.lower():
                logging_module.error(
                    "push_rejected",
                    branch=branch,
                    error="Push rejected by remote",
                )
            else:
                logging_module.error(
                    "push_failed",
                    branch=branch,
                    error=error_output,
                )
            return False

    except Exception as e:
        logging_module.error(
            "push_error",
            branch=branch,
            error=str(e),
        )
        return False


def commit_and_push(
    files: List[str],
    message: str,
    branch: str = "main",
) -> bool:
    """Stage files, commit, and push.

    Args:
        files: List of file paths
        message: Commit message
        branch: Branch to push to

    Returns:
        True if successful, False otherwise
    """
    try:
        # Stage files
        if not stage_files(files):
            return False

        # Commit
        commit_hash = commit(message)
        if not commit_hash:
            # No changes to commit is not necessarily a failure
            logging_module.info(
                "commit_and_push_skipped",
                reason="no_changes",
            )
            return True

        # Push
        if not push(branch):
            logging_module.error(
                "commit_and_push_failed",
                reason="push_failed",
            )
            return False

        logging_module.info(
            "commit_and_push_success",
            commit=commit_hash,
            branch=branch,
        )
        return True

    except Exception as e:
        logging_module.error(
            "commit_and_push_error",
            error=str(e),
        )
        return False
