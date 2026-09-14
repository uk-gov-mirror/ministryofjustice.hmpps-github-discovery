"""Deployment analytics helpers.

This module collects deployment metrics for HMPPS repositories, flattens the
results into CSV rows, writes JSON manifests, and optionally uploads artifacts
to SharePoint for reporting.

Required environment variables
------------------------------

Github (Credentials for Discovery app that has access to the repositories)
- GITHUB_APP_ID: Github App ID
- GITHUB_APP_INSTALLATION_ID: Github App Installation ID
- GITHUB_APP_PRIVATE_KEY: Github App Private Key

Service Catalogue
- SERVICE_CATALOGUE_API_ENDPOINT: Service Catalogue API endpoint
- SERVICE_CATALOGUE_API_KEY: Service Catalogue API key

SharePoint / Microsoft Graph
- SP_CLIENT_ID: SharePoint application client ID
- SP_CLIENT_SECRET: SharePoint application client secret
- AZ_TENANT_ID: Azure tenant ID
- SITE_NAME: SharePoint site name for upload targets (default: HMPPSSRE)

Optional environment variables
- UPLOAD: Upload generated analytics files to SharePoint (default: false)
- DRIVE_NAME: SharePoint drive name (default: Documents)
- FOLDER_PATH: SharePoint folder path for analysis exports
                             (default: analytics/deployments)
- PARTITION_BY_DATE: Partition output by year/month (default: true)
- LOG_LEVEL: Log level (default: INFO)
"""

from datetime import datetime, timezone
import csv
import json
import re
from pathlib import Path
from statistics import mean, median
from time import sleep

from dateutil.relativedelta import relativedelta
from hmpps import GithubSession, ServiceCatalogue, SharePoint
from hmpps.services.job_log_handling import log_error, log_info, log_debug


SERVICE_METRICS_COLUMNS = [
  'report_month',
  'snapshot_at',
  'service_key',
  'product',
  'product_name',
  'monorepo',
  'total_prs',
  'prs_with_deployments',
  'successful_deployments',
  'error_deployments',
  'other_deployments',
  'revert_count',
  'avg_minutes_to_deploy',
  'median_minutes_to_deploy',
  'deployment_coverage_rate',
  'deployment_success_rate',
  'revert_rate',
  'error_count',
]

DEPLOYMENT_EVENTS_COLUMNS = [
  'report_month',
  'snapshot_at',
  'service_key',
  'product',
  'product_name',
  'monorepo',
  'pr_number',
  'merged_at',
  'deployed_at',
  'duration_seconds',
  'duration_minutes',
  'status',
  'was_successful',
  'was_error',
]

REVERTS_COLUMNS = [
  'report_month',
  'snapshot_at',
  'service_key',
  'product',
  'product_name',
  'monorepo',
  'revert_pr_number',
  'referenced_pr',
  'merged_at',
  'title',
  'url',
]


OUTPUT_PATH = Path('deployments.json')
OUTPUT_DIR = Path('analytics')

DEFAULT_RUNTIME_CONFIG = {
  'upload': False,
  'site_name': 'HMPPSSRE',
  'drive_name': 'Documents',
  'folder_path': 'analytics/deployments',
  'partition_by_date': True,
}

MAX_GH_RETRIES = 4
MAX_UPLOAD_RETRIES = 4
RETRY_BASE_SECONDS = 5
MAX_RETRY_SLEEP_SECONDS = 60


# This section is all about writing the files in various formats
def _write_json_atomic(path: Path, payload: dict) -> None:
  """Write a JSON payload atomically by replacing the final file once complete."""
  temp_path = path.with_suffix(f'{path.suffix}.tmp')
  with temp_path.open('w') as f:
    json.dump(payload, f, indent=2)
  temp_path.replace(path)


def write_csv(path: Path, rows: list[dict], columns: list[str]) -> None:
  """Persist a list of row dictionaries as CSV using the supplied column ordering."""
  path.parent.mkdir(parents=True, exist_ok=True)
  with path.open('w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)


def write_json(path: Path, payload: dict) -> None:
  """Write a JSON payload to disk, creating parent directories as required."""
  path.parent.mkdir(parents=True, exist_ok=True)
  _write_json_atomic(path, payload)


# Error handling
def classify_github_error(error: Exception) -> tuple[str, int | None]:
  """Classify a GitHub API exception as temporary or permanent for retry handling."""
  status = getattr(error, 'status', None) or getattr(error, 'status_code', None)
  message = str(error).lower()

  if status == 404:
    return 'permanent', status
  if status in {429, 500, 502, 503, 504}:
    return 'temporary', status
  if status in {401, 403}:
    if 'rate limit' in message or 'secondary rate limit' in message:
      return 'temporary', status
    return 'permanent', status

  if any(
    token in message
    for token in [
      'timed out',
      'timeout',
      'temporarily unavailable',
      'connection reset',
      'connection aborted',
      'connection refused',
      'remote disconnected',
      'service unavailable',
    ]
  ):
    return 'temporary', status

  return 'permanent', status


def classify_upload_error(error: Exception) -> str:
  """Decide if a SharePoint upload failure should be retried or treated as permanent."""
  status = getattr(error, 'status', None) or getattr(error, 'status_code', None)
  message = str(error).lower()

  if status in {429, 500, 502, 503, 504}:
    return 'temporary'

  if any(
    token in message
    for token in [
      'timed out',
      'timeout',
      'temporarily unavailable',
      'connection reset',
      'connection aborted',
      'connection refused',
      'remote disconnected',
      'service unavailable',
      'throttle',
      'too many requests',
      'rate limit',
    ]
  ):
    return 'temporary'

  return 'permanent'


def run_call_with_retries(operation: str, fn, classify_error, max_retries: int):
  """Run a GitHub or upload call with exponential backoff for transient failures."""
  for attempt in range(1, max_retries + 1):
    try:
      return fn(), None
    except Exception as error:
      error_meta = classify_error(error)
      if isinstance(error_meta, tuple):
        error_type = error_meta[0]
        status = error_meta[1]
      else:
        error_type = error_meta
        status = getattr(error, 'status', None) or getattr(error, 'status_code', None)

      if error_type == 'temporary' and attempt < max_retries:
        sleep_seconds = min(
          RETRY_BASE_SECONDS * (2 ** (attempt - 1)), MAX_RETRY_SLEEP_SECONDS
        )
        log_info(
          f'{operation} failed with temporary error (status={status}, '
          f'attempt={attempt}/{max_retries}). Retrying in {sleep_seconds}s.'
        )
        sleep(sleep_seconds)
        continue

      return None, {
        'operation': operation,
        'error_type': error_type,
        'status': status,
        'attempts': attempt,
        'message': str(error),
      }

  return None, {
    'operation': operation,
    'error_type': 'temporary',
    'status': None,
    'attempts': max_retries,
    'message': 'Retry loop exhausted unexpectedly.',
  }


def format_error_for_log(error_info: dict) -> str:
  """Convert an error payload into a single readable log line for diagnostics."""
  return (
    f'{error_info.get("operation")} | '
    f'type={error_info.get("error_type")} | '
    f'status={error_info.get("status")} | '
    f'attempts={error_info.get("attempts")} | '
    f'message={error_info.get("message")}'
  )


# Rate checking - very similar to what we do with Github discovery
def check_gh_rate(gh: GithubSession) -> None:
  """Sleep when the GitHub API approaches quota to avoid rate-limit errors."""
  cur_rate_limit = gh.get_rate_limit()
  if cur_rate_limit:
    log_info(
      f'Github API rate limit {cur_rate_limit.remaining} / {cur_rate_limit.limit} '
      f'remains - resets at {cur_rate_limit.reset}'
    )
  else:
    gh.auth()
    cur_rate_limit = gh.get_rate_limit()

  while cur_rate_limit and cur_rate_limit.remaining < 500:
    time_delta = cur_rate_limit.reset - datetime.now(timezone.utc)
    time_to_reset = int(time_delta.total_seconds())
    sleep_seconds = max(time_to_reset + 10, 1)
    log_info(
      f'Backing off for {sleep_seconds} seconds to avoid GitHub API limits.'
    )
    sleep(sleep_seconds)
    log_debug('Reauthenticating')
    gh.auth()
    cur_rate_limit = gh.get_rate_limit()
    if not cur_rate_limit:
      gh.auth()
      cur_rate_limit = gh.get_rate_limit()


# Deployment analysis section
#############################
def is_revert(pr, repo) -> tuple[bool, str]:
  """Check if a PR looks like a revert and return the ref PR number if found."""

  def referenced_pr_from(text: str) -> str:
    match = re.search(r'#(\d+)', text)
    return match.group(1) if match else ''

  if re.match(r'^\s*revert\b', pr.title, re.IGNORECASE):
    return True, referenced_pr_from(pr.title)

  try:
    if pr.merge_commit_sha:
      commit = repo.get_commit(pr.merge_commit_sha)
      if commit.commit and commit.commit.message:
        subject = commit.commit.message.splitlines()[0]
        if re.match(r'^\s*revert\b', subject, re.IGNORECASE):
          return True, referenced_pr_from(subject)
  except Exception:
    pass

  return False, ''


def format_duration(duration_seconds: float) -> str:
  """Render a duration in a readable day/hour/minute/second
  format for deployment logs."""
  days = int(duration_seconds // 86400)
  hours = int((duration_seconds % 86400) // 3600)
  minutes = int((duration_seconds % 3600) // 60)
  seconds = int(duration_seconds % 60)
  return f'{days}d {hours:02d}:{minutes:02d}:{seconds:02d}'


def build_metrics(report: dict) -> dict:
  """Aggregate deploy-time metrics into average and median
  durations for the reporting summary."""
  durations_seconds = [
    record['duration_seconds']
    for record in report['merge_to_deploy_times']
    if record['duration_seconds'] is not None
  ]

  metrics = {
    'prs_with_deployments': report['prs_with_deployments'],
    'successful_deployments': report['deployment_stats']['success'],
    'revert_count': len(report['reverts']),
    'time_to_deploy': {
      'average_seconds': None,
      'average_minutes': None,
      'median_seconds': None,
      'median_minutes': None,
    },
  }

  if durations_seconds:
    avg_seconds = mean(durations_seconds)
    med_seconds = median(durations_seconds)
    metrics['time_to_deploy'] = {
      'average_seconds': round(avg_seconds, 2),
      'average_minutes': round(avg_seconds / 60, 2),
      'median_seconds': round(med_seconds, 2),
      'median_minutes': round(med_seconds / 60, 2),
    }

  return metrics


def get_push_triggered_run_ids(repo, commit_sha: str) -> set[int]:
  """Return the workflow run IDs triggered by a push on a given commit SHA."""
  runs, error = run_call_with_retries(
    operation=f'get workflow runs for commit {commit_sha}',
    fn=lambda: repo.get_workflow_runs(head_sha=commit_sha, event='push'),
    classify_error=classify_github_error,
    max_retries=MAX_GH_RETRIES,
  )
  if error or runs is None:
    log_error(
      format_error_for_log(
        error
        or {
          'operation': f'get workflow runs for commit {commit_sha}',
          'error_type': 'permanent',
          'status': None,
          'attempts': 1,
          'message': 'GitHub API returned no workflow runs iterator.',
        }
      )
    )
    return set()

  try:
    return {run.id for run in runs}
  except Exception:
    return set()


def extract_run_id(log_url: str | None) -> int | None:
  """Extract the workflow run ID from a GitHub Actions log URL when present."""
  if not log_url:
    return None
  try:
    return int(log_url.split('/runs/')[1].split('/')[0])
  except (IndexError, ValueError):
    return None


def get_deployment_stats(
  gh: GithubSession,
  repo_name: str,
  since_dt: datetime,
  until_dt: datetime | None = None,
) -> dict:
  """Collect deployment-related PR stats for a single repository over a time window."""
  repo, repo_error = run_call_with_retries(
    operation=f'load repository {repo_name}',
    fn=lambda: gh.get_org_repo(repo_name),
    classify_error=classify_github_error,
    max_retries=MAX_GH_RETRIES,
  )
  if repo_error:
    return {'status': 'failed', 'report': None, 'error': repo_error}
  if not repo:
    return {
      'status': 'failed',
      'report': None,
      'error': {
        'operation': f'load repository {repo_name}',
        'error_type': 'permanent',
        'status': 404,
        'attempts': 1,
        'message': 'Repository not found or inaccessible.',
      },
    }

  envs, env_error = run_call_with_retries(
    operation=f'get environments for {repo_name}',
    fn=lambda: list(repo.get_environments()),
    classify_error=classify_github_error,
    max_retries=MAX_GH_RETRIES,
  )
  if env_error:
    return {'status': 'failed', 'report': None, 'error': env_error}
  if not envs:
    return {'status': 'no_data', 'report': None, 'error': None}

  prod_env = next((env.name for env in envs if env.name.startswith('prod')), None)
  if not prod_env:
    log_info(f'No production environment found for {repo_name}')
    return {'status': 'no_data', 'report': None, 'error': None}

  report = {
    'total_prs': 0,
    'prs_with_deployments': 0,
    'deployment_stats': {'success': 0, 'error': 0, 'other': 0},
    'merge_to_deploy_times': [],
    'reverts': [],
    'errors': [],
    'metrics': {},
  }

  prs, prs_error = run_call_with_retries(
    operation=f'get pull requests for {repo_name}',
    fn=lambda: repo.get_pulls(state='closed', sort='updated', direction='desc'),
    classify_error=classify_github_error,
    max_retries=MAX_GH_RETRIES,
  )
  if prs_error or prs is None:
    return {
      'status': 'failed',
      'report': None,
      'error': prs_error
      or {
        'operation': f'get pull requests for {repo_name}',
        'error_type': 'permanent',
        'status': None,
        'attempts': 1,
        'message': 'GitHub API returned no pull request iterator.',
      },
    }

  try:
    for pr in prs:
      if not pr.merged_at:
        continue
      updated_at = getattr(pr, 'updated_at', None)
      if updated_at and updated_at < since_dt:
        break
      if pr.merged_at < since_dt:
        continue
      if until_dt and pr.merged_at >= until_dt:
        continue

      report['total_prs'] += 1

      is_revert_pr, referenced_pr = is_revert(pr, repo)
      if is_revert_pr:
        report['reverts'].append(
          {
            'pr_number': pr.number,
            'title': pr.title,
            'merged_at': pr.merged_at.strftime('%Y-%m-%d %H:%M:%S')
            if pr.merged_at
            else None,
            'referenced_pr': referenced_pr if referenced_pr else None,
            'url': pr.html_url,
          }
        )
        continue

      commit_sha = pr.merge_commit_sha
      if not commit_sha:
        report['errors'].append(f'PR #{pr.number}: No merge commit SHA')
        continue

      dep_page, dep_error = run_call_with_retries(
        operation=f'get deployments for {repo_name} PR #{pr.number}',
        fn=lambda: repo.get_deployments(sha=commit_sha, environment=prod_env),
        classify_error=classify_github_error,
        max_retries=MAX_GH_RETRIES,
      )
      if dep_error or dep_page is None:
        fallback = {
          'operation': f'get deployments for {repo_name} PR #{pr.number}',
          'error_type': 'permanent',
          'status': None,
          'attempts': 1,
          'message': 'GitHub API returned no deployments iterator.',
        }
        report['errors'].append(
          f'PR #{pr.number}: {format_error_for_log(dep_error or fallback)}'
        )
        continue

      deps_list = list(dep_page)
      if not deps_list:
        continue

      push_run_ids = get_push_triggered_run_ids(repo, commit_sha)
      report['prs_with_deployments'] += 1

      for dep in deps_list:
        if dep.created_at < since_dt:
          continue
        if until_dt and dep.created_at >= until_dt:
          continue

        merge_time = pr.merged_at
        deploy_time = dep.created_at

        statuses, status_error = run_call_with_retries(
          operation=f'get deployment statuses for {repo_name} PR #{pr.number}',
          fn=lambda: list(dep.get_statuses()),
          classify_error=classify_github_error,
          max_retries=MAX_GH_RETRIES,
        )
        if status_error or statuses is None:
          fallback = {
            'operation': f'get deployment statuses for {repo_name} PR #{pr.number}',
            'error_type': 'permanent',
            'status': None,
            'attempts': 1,
            'message': 'GitHub API returned no deployment statuses.',
          }
          report['errors'].append(
            f'PR #{pr.number}: {format_error_for_log(status_error or fallback)}'
          )
          continue

        states = {s.state for s in statuses}
        in_progress_status = next(
          (s for s in statuses if s.state == 'in_progress'), None
        )

        if in_progress_status:
          run_id = extract_run_id(in_progress_status.log_url)
          if push_run_ids and run_id not in push_run_ids:
            continue
          duration_seconds = (
            in_progress_status.created_at - merge_time
          ).total_seconds()
          time_to_deploy = format_duration(duration_seconds)
        else:
          duration_seconds = None
          time_to_deploy = '—'

        if 'success' in states:
          report['deployment_stats']['success'] += 1
        elif 'error' in states:
          report['deployment_stats']['error'] += 1
        else:
          report['deployment_stats']['other'] += 1

        report['merge_to_deploy_times'].append(
          {
            'pr_number': pr.number,
            'merged_at': merge_time.strftime('%Y-%m-%d %H:%M:%S'),
            'time_to_deploy': time_to_deploy,
            'duration_seconds': duration_seconds,
            'deployed_at': deploy_time.strftime('%Y-%m-%d %H:%M:%S'),
            'status': ','.join(sorted(states)),
          }
        )
  except Exception as error:
    return {
      'status': 'failed',
      'report': None,
      'error': {
        'operation': f'collect deployment stats for {repo_name}',
        'error_type': 'permanent',
        'status': None,
        'attempts': 1,
        'message': str(error),
      },
    }

  report['metrics'] = build_metrics(report)
  return {'status': 'success', 'report': report, 'error': None}


def safe_ratio(numerator: float, denominator: float) -> float | None:
  """Return a rounded percentage-like ratio, or None when the denominator is zero."""
  if not denominator:
    return None
  return round(numerator / denominator, 4)


def flatten_deployments(
  deployments: dict, snapshot_at: str, report_month: str | None = None
) -> tuple[list[dict], list[dict], list[dict]]:
  """Convert per-service deployment summaries into CSV-ready row dictionaries."""
  service_metrics_rows: list[dict] = []
  deployment_event_rows: list[dict] = []
  revert_rows: list[dict] = []

  for service_key, service_data in deployments.items():
    if not service_data:
      continue

    metrics = service_data.get('metrics', {})
    deploy_stats = service_data.get('deployment_stats', {})
    product = service_data.get('product', '')
    product_name = service_data.get('product_name', '')
    monorepo = bool(service_data.get('monorepo', False))
    total_prs = service_data.get('total_prs', 0)
    prs_with_deployments = service_data.get('prs_with_deployments', 0)
    successful_deployments = metrics.get(
      'successful_deployments', deploy_stats.get('success', 0)
    )
    error_deployments = deploy_stats.get('error', 0)
    other_deployments = deploy_stats.get('other', 0)
    total_deployments = successful_deployments + error_deployments + other_deployments
    revert_count = metrics.get('revert_count', len(service_data.get('reverts', [])))
    error_count = len(service_data.get('errors', []))

    service_metrics_rows.append(
      {
        'report_month': report_month,
        'snapshot_at': snapshot_at,
        'service_key': service_key,
        'product': product,
        'product_name': product_name,
        'monorepo': monorepo,
        'total_prs': total_prs,
        'prs_with_deployments': prs_with_deployments,
        'successful_deployments': successful_deployments,
        'error_deployments': error_deployments,
        'other_deployments': other_deployments,
        'revert_count': revert_count,
        'avg_minutes_to_deploy': metrics.get('time_to_deploy', {}).get(
          'average_minutes'
        ),
        'median_minutes_to_deploy': metrics.get('time_to_deploy', {}).get(
          'median_minutes'
        ),
        'deployment_coverage_rate': safe_ratio(prs_with_deployments, total_prs),
        'deployment_success_rate': safe_ratio(
          successful_deployments, total_deployments
        ),
        'revert_rate': safe_ratio(revert_count, total_prs),
        'error_count': error_count,
      }
    )

    for event in service_data.get('merge_to_deploy_times', []):
      status = event.get('status', '')
      duration_seconds = event.get('duration_seconds')
      deployment_event_rows.append(
        {
          'report_month': report_month,
          'snapshot_at': snapshot_at,
          'service_key': service_key,
          'product': product,
          'product_name': product_name,
          'monorepo': monorepo,
          'pr_number': event.get('pr_number'),
          'merged_at': event.get('merged_at'),
          'deployed_at': event.get('deployed_at'),
          'duration_seconds': duration_seconds,
          'duration_minutes': round(duration_seconds / 60, 2)
          if duration_seconds is not None
          else None,
          'status': status,
          'was_successful': 'success' in status,
          'was_error': 'error' in status,
        }
      )

    for revert in service_data.get('reverts', []):
      revert_rows.append(
        {
          'report_month': report_month,
          'snapshot_at': snapshot_at,
          'service_key': service_key,
          'product': product,
          'product_name': product_name,
          'monorepo': monorepo,
          'revert_pr_number': revert.get('pr_number'),
          'referenced_pr': revert.get('referenced_pr'),
          'merged_at': revert.get('merged_at'),
          'title': revert.get('title'),
          'url': revert.get('url'),
        }
      )

  return service_metrics_rows, deployment_event_rows, revert_rows


def build_month_partition(period_start: datetime) -> str:
  """Build the folder partition path used when output is grouped by month."""
  return f'year={period_start:%Y}/month={period_start:%m}'


def resolve_reporting_window(
  run_at: datetime,
  since_dt: datetime | None,
  until_dt: datetime | None,
) -> tuple[datetime, datetime]:
  """Resolve the month or custom time window used to collect deployment statistics."""
  if since_dt is None and until_dt is None:
    this_month_start = run_at.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start = this_month_start - relativedelta(months=1)
    end = start + relativedelta(months=1)
    return start, end

  if since_dt is None:
    if until_dt is None:
      raise ValueError('until_dt is required when since_dt is not provided')
    end = until_dt
    start = (end - relativedelta(months=1)).replace(
      hour=0, minute=0, second=0, microsecond=0
    )
    return start, end

  if until_dt is None:
    start = since_dt
    end = start + relativedelta(months=1)
    return start, end

  return since_dt, until_dt


def upload_file_with_retries(
  sp: SharePoint, drive_name: str, folder_path: str, file_path: Path
) -> tuple[bool, str | None]:
  """Upload a generated report file to SharePoint with retry for transient failures."""
  for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
    last_error: Exception | None = None
    try:
      ok = sp.upload_file(
        drive_name=drive_name,
        folder_path=folder_path,
        local_file_path=str(file_path),
      )
      if ok:
        return True, None
      last_error = RuntimeError(
        f'Upload returned false for {file_path.name} without exception.'
      )
    except Exception as error:
      last_error = error

    error_kind = classify_upload_error(last_error)
    if error_kind == 'temporary' and attempt < MAX_UPLOAD_RETRIES:
      sleep_seconds = min(
        RETRY_BASE_SECONDS * (2 ** (attempt - 1)), MAX_RETRY_SLEEP_SECONDS
      )
      log_info(
        f'Upload failed for {file_path.name} with temporary error '
        f'(attempt={attempt}/{MAX_UPLOAD_RETRIES}). Retrying in {sleep_seconds}s.'
      )
      sleep(sleep_seconds)
      continue

    return (
      False,
      (
        f'file={file_path.name} | type={error_kind} | attempts={attempt} | '
        f'message={str(last_error)}'
      ),
    )

  return (
    False,
    f'file={file_path.name} | type=temporary | attempts={MAX_UPLOAD_RETRIES}',
  )


# Main function for looping through components and collecting deployments from Github
####################################################################################
def collect_deployments(
  gh: GithubSession,
  sc: ServiceCatalogue,
  since_dt: datetime,
  until_dt: datetime | None = None,
) -> tuple[dict, dict, dict[str, str]]:
  """Collect deployment stats for every component in Service Catalogue
  for the chosen window."""
  deployments: dict = {}
  failed_components: dict[str, str] = {}
  components = sc.get_all_records(f'{sc.components_get}&filters[archived][$eq]=false')

  summary = {
    'total_components': len(components),
    'attempted': 0,
    'components_with_data': 0,
    'components_without_data': 0,
    'components_failed': 0,
    'failed_temporary': 0,
    'failed_permanent': 0,
    'saved_service_records': 0,
  }

  seen_keys: set[str] = set()

  for idx, component in enumerate(components, start=1):
    check_gh_rate(gh)

    component_name = component.get('name')
    monorepo = component.get('part_of_monorepo') or False
    github_repo = component.get('github_repo')
    p_id = component.get('product', {}).get('p_id', '')
    product_name = component.get('product', {}).get('name', '')
    key = github_repo if monorepo else component_name

    if key in seen_keys:
      continue
    seen_keys.add(key)

    summary['attempted'] += 1
    rate = gh.get_rate_limit()
    remaining_rate = rate.remaining if rate else 'unknown'
    log_info(
      f'Getting deployment stats for {github_repo}: {idx}/{len(components)} '
      f'({int(idx / len(components) * 100)}%) - {remaining_rate}'
    )

    result = get_deployment_stats(gh, github_repo, since_dt, until_dt)
    status = result.get('status')
    if status == 'success':
      stats = result.get('report')
      if stats:
        stats['product'] = p_id
        stats['product_name'] = product_name
        stats['monorepo'] = monorepo
        deployments[key] = stats
        summary['components_with_data'] += 1
    elif status == 'no_data':
      summary['components_without_data'] += 1
    else:
      error_info = result.get('error') or {
        'operation': f'collect deployment stats for {github_repo}',
        'error_type': 'permanent',
        'status': None,
        'attempts': 1,
        'message': 'Unknown failure',
      }
      failure_message = format_error_for_log(error_info)
      failed_components[key] = failure_message
      summary['components_failed'] += 1
      if error_info.get('error_type') == 'temporary':
        summary['failed_temporary'] += 1
      else:
        summary['failed_permanent'] += 1

    summary['saved_service_records'] = len(deployments)

  return deployments, summary, failed_components


# Main function to get the stats - this is called by github_deployment_analytics.py
###################################################################################
def run_deployments_pipeline(
  config: dict | None = None,
  since_dt: datetime | None = None,
  until_dt: datetime | None = None,
  report_month: str | None = None,
) -> dict:
  """Run the full deployment analytics job: collect, flatten,
  write, and optionally upload reports."""

  # 1 - get all the parameters set up
  config = {**DEFAULT_RUNTIME_CONFIG, **(config or {})}
  run_at = datetime.now(timezone.utc)
  snapshot_at = run_at.isoformat()
  window_start, window_end = resolve_reporting_window(run_at, since_dt, until_dt)

  if window_end <= window_start:
    raise ValueError('until_dt must be later than since_dt')

  if report_month is None and since_dt is None and until_dt is None:
    report_month = window_start.strftime('%Y-%m')

  partition_suffix = (
    build_month_partition(window_start) if config['partition_by_date'] else ''
  )
  output_dir = OUTPUT_DIR / partition_suffix if partition_suffix else OUTPUT_DIR

  gh = GithubSession()
  sc = ServiceCatalogue()

  # 2 - collect the deployments
  deployments, collection_summary, failed_components = collect_deployments(
    gh=gh,
    sc=sc,
    since_dt=window_start,
    until_dt=window_end,
  )

  # 3 - process the results and write the json & CSV
  write_json(OUTPUT_PATH, deployments)

  service_metrics_rows, deployment_event_rows, revert_rows = flatten_deployments(
    deployments, snapshot_at, report_month
  )

  service_metrics_path = output_dir / 'service_metrics.csv'
  deployment_events_path = output_dir / 'deployment_events.csv'
  reverts_path = output_dir / 'reverts.csv'
  manifest_path = output_dir / 'manifest.json'

  write_csv(service_metrics_path, service_metrics_rows, SERVICE_METRICS_COLUMNS)
  write_csv(deployment_events_path, deployment_event_rows, DEPLOYMENT_EVENTS_COLUMNS)
  write_csv(reverts_path, revert_rows, REVERTS_COLUMNS)

  manifest = {
    'generated_at': snapshot_at,
    'report_month': report_month,
    'window_start': window_start.isoformat(),
    'window_end': window_end.isoformat(),
    'input_file': str(OUTPUT_PATH),
    'partition': partition_suffix or None,
    'rows': {
      'service_metrics': len(service_metrics_rows),
      'deployment_events': len(deployment_event_rows),
      'reverts': len(revert_rows),
    },
    'collection_summary': collection_summary,
  }
  write_json(manifest_path, manifest)

  output_files = [
    service_metrics_path,
    deployment_events_path,
    reverts_path,
    manifest_path,
  ]

  # 4 - upload the summary if configured to do so
  upload_summary = None
  uploaded = False
  if config['upload']:
    sp = SharePoint(site_name=config['site_name'])
    upload_folder_path = config['folder_path']
    if partition_suffix:
      upload_folder_path = (
        f'{config["folder_path"].rstrip("/")}/{partition_suffix}'
        if config['folder_path']
        else partition_suffix
      )

    files_uploaded = 0
    failed_files: dict[str, str] = {}
    for file_path in output_files:
      ok, error_message = upload_file_with_retries(
        sp=sp,
        drive_name=config['drive_name'],
        folder_path=upload_folder_path,
        file_path=file_path,
      )
      if ok:
        files_uploaded += 1
      else:
        failed_files[str(file_path)] = error_message or 'Unknown upload error.'

    upload_summary = {
      'files_total': len(output_files),
      'files_uploaded': files_uploaded,
      'files_failed': len(failed_files),
      'failed_files': failed_files,
    }
    uploaded = upload_summary['files_failed'] == 0

  status = 'success'
  errors: list[str] = []
  if failed_components:
    status = 'failed'
    errors.append(f'{len(failed_components)} component(s) failed during collection.')
  if config['upload'] and upload_summary and upload_summary['files_failed'] > 0:
    status = 'failed'
    errors.append(f'{upload_summary["files_failed"]} upload(s) failed after retries.')

  result = {
    'status': status,
    'output_files': [str(path) for path in output_files],
    'uploaded': uploaded,
    'drive_name': config['drive_name'] if config['upload'] else None,
    'folder_path': (
      f'{config["folder_path"].rstrip("/")}/{partition_suffix}'
      if config['upload'] and partition_suffix and config['folder_path']
      else (
        partition_suffix
        if config['upload'] and partition_suffix
        else config['folder_path']
        if config['upload']
        else None
      )
    ),
    'partition': partition_suffix or None,
    'report_month': report_month,
    'window_start': window_start.isoformat(),
    'window_end': window_end.isoformat(),
    'summary': {
      'collection': collection_summary,
      'rows': manifest['rows'],
      'failed_components': failed_components,
      'upload': upload_summary,
      'errors': errors,
    },
    'config': config,
  }

  log_info('Deployment pipeline summary:')
  log_info(json.dumps(result['summary'], indent=2))
  if failed_components:
    log_error('Failed components:')
    log_error(json.dumps(failed_components, indent=2))

  return result
