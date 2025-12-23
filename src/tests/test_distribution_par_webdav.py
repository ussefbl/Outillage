# tests/test_distribution_par_webdav.py
import os
import stat
import csv
import importlib
import logging
from pathlib import Path

import pytest


# -------- Helpers --------

def _make_logger(name="test-logger"):
    log = logging.getLogger(name)
    log.handlers.clear()
    log.setLevel(logging.DEBUG)
    log.addHandler(logging.NullHandler())
    return log


def _write_csv(path: Path, rows, header=None, sep=";"):
    """
    rows: list[dict] or list[list]
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if header is None and isinstance(rows[0], dict):
        header = list(rows[0].keys())

    with path.open("w", encoding="utf-8", newline="") as f:
        if isinstance(rows[0], dict):
            w = csv.DictWriter(f, fieldnames=header, delimiter=sep)
            w.writeheader()
            for r in rows:
                w.writerow(r)
        else:
            w = csv.writer(f, delimiter=sep)
            if header:
                w.writerow(header)
            for r in rows:
                w.writerow(r)


def _touch(p: Path, content: bytes = b"X"):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)
    return p


def _mk_yyyymmdd_dir(base: Path, yyyymmdd: str):
    d = base / yyyymmdd
    d.mkdir(parents=True, exist_ok=True)
    return d


def _reload_module():
    import distribustion_par_webdav as m
    importlib.reload(m)
    return m


@pytest.fixture()
def mod(tmp_path):
    """
    Recharge le module à chaque test et initialise des globals cohérents.
    """
    m = _reload_module()
    m.logger = _make_logger()

    # Defaults tests
    m.param_date_traitement = "20251223"
    m.param_mode_copie = ""          # non full
    m.MODE_COPY_PAR = "CLEVA"        # tu dis que tu le gères "en dur" par domaine
    m.param_interface_path = str(tmp_path / "interfaces") + "/"
    m.param_webdav_path = str(tmp_path / "webdav") + "/"
    m.param_logshell_path = ""       # pas utilisé ici
    m.param_log_verbose = "INFO"

    # mapping paths
    m.param_ref_mapping_path = str(tmp_path / "mapping.csv")

    # Override homes to avoid accidental real FS
    m.CLEVA_DATA_HOME = m.param_interface_path
    m.DSN_DATA_HOME = m.param_interface_path
    m.WEBDAV_HOME = m.param_webdav_path

    return m


# -------- Tests: Plan (CSV -> copy_plan) --------

def test_plan_config_not_found(mod):
    mod.param_ref_mapping_path = str(Path(mod.param_ref_mapping_path).with_name("missing.csv"))
    plan, rc = mod.prepare_copy_plan_from_reference()
    assert plan == []
    assert rc == mod.RC_CONFIG_NOT_FOUND


def test_plan_config_invalid_missing_required_columns(mod):
    _write_csv(Path(mod.param_ref_mapping_path), rows=[
        {"foo": "bar"},
    ])
    plan, rc = mod.prepare_copy_plan_from_reference()
    assert plan == []
    assert rc == mod.RC_CONFIG_INVALID


def test_plan_ignores_row_when_source_dir_missing(mod, tmp_path):
    # mapping expects interfaces/<source> to exist
    _write_csv(Path(mod.param_ref_mapping_path), rows=[{
        "type": "CLEVA",
        "source": "in/doesnotexist",
        "destination": "pars/CCO/WAIT",
        "prefix01": "*.par",
    }])

    plan, rc = mod.prepare_copy_plan_from_reference()
    assert plan == []
    assert rc == mod.RC_NOTHING_TO_DO


def test_plan_prefix_only_matches_files(mod, tmp_path):
    src = tmp_path / "interfaces" / "in" / "flow"
    _touch(src / "A.par", b"a")
    _touch(src / "B.par", b"b")
    _touch(src / "note.txt", b"n")

    _write_csv(Path(mod.param_ref_mapping_path), rows=[{
        "type": "CLEVA",
        "source": "in/flow",
        "destination": "pars/CCO/WAIT",
        "prefix01": "*.par",          # no extension columns => treated as full mask
    }])

    plan, rc = mod.prepare_copy_plan_from_reference()
    assert rc == mod.RC_OK
    assert len(plan) == 1
    assert plan[0]["files"] == ["A.par", "B.par"]


def test_plan_prefix_and_extension_builds_combinations(mod, tmp_path):
    src = tmp_path / "interfaces" / "in" / "flow"
    _touch(src / "X_001.par", b"x")
    _touch(src / "Y_001.par", b"y")
    _touch(src / "X_001.xml", b"z")

    _write_csv(Path(mod.param_ref_mapping_path), rows=[{
        "type": "CLEVA",
        "source": "in/flow",
        "destination": "pars/CCO/WAIT",
        "prefix01": "X_*",
        "extension01": ".par",
    }])

    plan, rc = mod.prepare_copy_plan_from_reference()
    assert rc == mod.RC_OK
    assert plan[0]["files"] == ["X_001.par"]


def test_plan_exclude_prefix_fnmatch(mod, tmp_path):
    src = tmp_path / "interfaces" / "in" / "flow"
    _touch(src / "keep.par", b"k")
    _touch(src / "bad_DSN-999.par", b"b")

    _write_csv(Path(mod.param_ref_mapping_path), rows=[{
        "type": "CLEVA",
        "source": "in/flow",
        "destination": "pars/CCO/WAIT",
        "prefix01": "*.par",
        "exclude_prefix01": "*DSN-*.par",
    }])

    plan, rc = mod.prepare_copy_plan_from_reference()
    assert rc == mod.RC_OK
    assert plan[0]["files"] == ["keep.par"]


def test_plan_latest_yyyymmdd_selects_latest(mod, tmp_path):
    base = tmp_path / "interfaces" / "in" / "arch"
    d1 = _mk_yyyymmdd_dir(base, "20250101")
    d2 = _mk_yyyymmdd_dir(base, "20251201")
    _touch(d1 / "A.par", b"a1")
    _touch(d2 / "A.par", b"a2")

    _write_csv(Path(mod.param_ref_mapping_path), rows=[{
        "type": "CLEVA",
        "source": "in/arch",
        "destination": "pars/CCO/WAIT",
        "prefix01": "*.par",
        "date_policy": "LATEST_YYYYMMDD",
    }])

    plan, rc = mod.prepare_copy_plan_from_reference()
    assert rc == mod.RC_OK
    # source path should be the latest folder
    assert plan[0]["source"].replace("\\", "/").endswith("/in/arch/20251201")
    assert plan[0]["files"] == ["A.par"]


def test_plan_latest_yyyymmdd_no_valid_subdir_ignored(mod, tmp_path):
    base = tmp_path / "interfaces" / "in" / "arch"
    (base / "bad").mkdir(parents=True, exist_ok=True)
    _touch(base / "bad" / "A.par", b"a")

    _write_csv(Path(mod.param_ref_mapping_path), rows=[{
        "type": "CLEVA",
        "source": "in/arch",
        "destination": "pars/CCO/WAIT",
        "prefix01": "*.par",
        "date_policy": "LATEST_YYYYMMDD",
    }])

    plan, rc = mod.prepare_copy_plan_from_reference()
    assert plan == []
    assert rc == mod.RC_NOTHING_TO_DO


def test_plan_star_prefix_removed_when_specific_exists(mod, tmp_path):
    src = tmp_path / "interfaces" / "in" / "flow"
    _touch(src / "AA.par", b"a")
    _touch(src / "BB.par", b"b")

    _write_csv(Path(mod.param_ref_mapping_path), rows=[{
        "type": "CLEVA",
        "source": "in/flow",
        "destination": "pars/CCO/WAIT",
        "prefix01": "*",
        "prefix02": "AA*",
        "extension01": ".par",
    }])

    plan, rc = mod.prepare_copy_plan_from_reference()
    assert rc == mod.RC_OK
    assert plan[0]["files"] == ["AA.par"]


# -------- Tests: Copy (copy_plan -> fs) --------

def test_copy_creates_destination_dir_and_copies_with_txt(mod, tmp_path):
    src = tmp_path / "interfaces" / "in" / "flow"
    _touch(src / "A.par", b"a")

    dest = tmp_path / "webdav" / "tech" / mod.param_date_traitement / "pars" / "CCO" / "WAIT"
    plan = [{
        "source": str(src),
        "destination": str(dest).replace("\\", "/"),
        "files": ["A.par"],
        "purge": False,
    }]

    total, rc = mod.copy_files_to_webdav(plan)
    assert rc == mod.RC_OK
    assert total == 1
    assert (dest / "A.par.txt").exists()
    assert (dest / "A.par.txt").read_bytes() == b"a"


def test_copy_does_not_double_txt(mod, tmp_path):
    src = tmp_path / "interfaces" / "in" / "flow"
    _touch(src / "A.par.txt", b"a")

    dest = tmp_path / "webdav" / "tech" / mod.param_date_traitement / "pars" / "CCO" / "WAIT"
    plan = [{
        "source": str(src),
        "destination": str(dest).replace("\\", "/"),
        "files": ["A.par.txt"],
        "purge": False,
    }]

    total, rc = mod.copy_files_to_webdav(plan)
    assert rc == mod.RC_OK
    assert (dest / "A.par.txt").exists()
    assert not (dest / "A.par.txt.txt").exists()


def test_copy_skip_when_destination_exists(mod, tmp_path):
    src = tmp_path / "interfaces" / "in" / "flow"
    _touch(src / "A.par", b"a")

    dest = tmp_path / "webdav" / "tech" / mod.param_date_traitement / "pars" / "CCO" / "DONE"
    (dest).mkdir(parents=True, exist_ok=True)
    _touch(dest / "A.par.txt", b"old")

    plan = [{
        "source": str(src),
        "destination": str(dest).replace("\\", "/"),
        "files": ["A.par"],
        "purge": False,
    }]

    total, rc = mod.copy_files_to_webdav(plan)
    assert rc == mod.RC_NOTHING_TO_DO
    assert total == 0
    assert (dest / "A.par.txt").read_bytes() == b"old"


def test_wait_policy_skip_if_equivalent_exists_in_done_and_wait_absent(mod, tmp_path):
    src = tmp_path / "interfaces" / "in" / "flow"
    _touch(src / "A.par", b"a")

    base = tmp_path / "webdav" / "tech" / mod.param_date_traitement / "pars" / "CCO"
    done = base / "DONE"
    wait = base / "WAIT"
    done.mkdir(parents=True, exist_ok=True)

    # Done already has A.par.txt
    _touch(done / "A.par.txt", b"a")

    plan = [{
        "source": str(src),
        "destination": str(wait).replace("\\", "/"),
        "files": ["A.par"],
        "purge": False,
    }]

    total, rc = mod.copy_files_to_webdav(plan)
    # Because DONE contains equivalent, WAIT should be skipped (no file created)
    assert rc == mod.RC_NOTHING_TO_DO
    assert total == 0
    assert not (wait / "A.par.txt").exists()


def test_wait_policy_removes_existing_wait_if_done_has_equivalent(mod, tmp_path):
    src = tmp_path / "interfaces" / "in" / "flow"
    _touch(src / "A.par", b"new")

    base = tmp_path / "webdav" / "tech" / mod.param_date_traitement / "pars" / "CCO"
    done = base / "DONE"
    wait = base / "WAIT"
    done.mkdir(parents=True, exist_ok=True)
    wait.mkdir(parents=True, exist_ok=True)

    _touch(done / "A.par.txt", b"done-content")
    _touch(wait / "A.par.txt", b"wait-content")  # pre-existing WAIT file (different)

    plan = [{
        "source": str(src),
        "destination": str(wait).replace("\\", "/"),
        "files": ["A.par"],
        "purge": False,
    }]

    total, rc = mod.copy_files_to_webdav(plan)
    assert rc == mod.RC_NOTHING_TO_DO
    assert total == 0
    # wait file should have been removed
    assert not (wait / "A.par.txt").exists()


def test_wait_policy_endtime_key_matches_prefix(mod, tmp_path):
    """
    DONE: AAA.par.txt
    WAIT tries to copy AAA_endtime_XXXX.par -> becomes AAA_endtime_XXXX.par.txt
    logical key -> AAA.par.txt -> should skip
    """
    src = tmp_path / "interfaces" / "in" / "flow"
    _touch(src / "AAA_endtime_20251223_010203.par", b"x")

    base = tmp_path / "webdav" / "tech" / mod.param_date_traitement / "pars" / "CCO"
    done = base / "DONE"
    wait = base / "WAIT"
    done.mkdir(parents=True, exist_ok=True)
    _touch(done / "AAA.par.txt", b"done")

    plan = [{
        "source": str(src),
        "destination": str(wait).replace("\\", "/"),
        "files": ["AAA_endtime_20251223_010203.par"],
        "purge": False,
    }]

    total, rc = mod.copy_files_to_webdav(plan)
    assert rc == mod.RC_NOTHING_TO_DO
    assert total == 0
    assert not (wait / "AAA_endtime_20251223_010203.par.txt").exists()


def test_done_priority_before_wait_in_sort(mod):
    """
    Vérifie le tri des tâches: DONE (0) avant WAIT (1).
    """
    tasks = [
        {"destination": "/x/WAIT"},
        {"destination": "/x/DONE"},
        {"destination": "/x/OTHER"},
    ]
    tasks.sort(key=mod.copy_task_priority)
    assert "/x/DONE" in tasks[0]["destination"]
    assert "/x/WAIT" in tasks[1]["destination"]


def test_copy_fails_if_source_missing_between_plan_and_copy(mod, tmp_path):
    src = tmp_path / "interfaces" / "in" / "flow"
    src.mkdir(parents=True, exist_ok=True)
    # plan mentions a file that does not exist
    dest = tmp_path / "webdav" / "tech" / mod.param_date_traitement / "pars" / "CCO" / "DONE"

    plan = [{
        "source": str(src),
        "destination": str(dest).replace("\\", "/"),
        "files": ["missing.par"],
        "purge": False,
    }]

    total, rc = mod.copy_files_to_webdav(plan)
    assert rc == mod.RC_RUNTIME_ERROR
    assert total == 0


def test_copy_fails_if_destination_not_writable(mod, tmp_path):
    """
    Simule un problème de permission sur la destination.
    (Sur certains FS/OS, chmod peut être ignoré; on skip si impossible)
    """
    src = tmp_path / "interfaces" / "in" / "flow"
    _touch(src / "A.par", b"a")

    dest = tmp_path / "webdav" / "tech" / mod.param_date_traitement / "pars" / "CCO" / "DONE"
    dest.mkdir(parents=True, exist_ok=True)

    # Retire l'écriture du dossier
    try:
        dest.chmod(stat.S_IREAD | stat.S_IEXEC)
    except Exception:
        pytest.skip("chmod non supporté ici")

    plan = [{
        "source": str(src),
        "destination": str(dest).replace("\\", "/"),
        "files": ["A.par"],
        "purge": False,
    }]

    total, rc = mod.copy_files_to_webdav(plan)

    # Restaure permissions pour cleanup
    try:
        dest.chmod(stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
    except Exception:
        pass

    assert rc == mod.RC_RUNTIME_ERROR
    assert total == 0


# -------- Tests: match_domain_destination exactitude --------

def test_match_domain_destination_exact_wait_and_done(mod, tmp_path):
    base_webdav = Path(mod.param_webdav_path)
    base_dir = base_webdav / mod.param_date_traitement

    wait = base_dir / "pars" / "CCO" / "WAIT"
    done = base_dir / "pars" / "CCO" / "DONE"

    domain, kind, w, d = mod.match_domain_destination(str(wait))
    assert domain == "CCO"
    assert kind == "WAIT"
    assert w.replace("\\", "/").endswith("/pars/CCO/WAIT")
    assert d.replace("\\", "/").endswith("/pars/CCO/DONE")

    domain, kind, w, d = mod.match_domain_destination(str(done))
    assert domain == "CCO"
    assert kind == "DONE"
