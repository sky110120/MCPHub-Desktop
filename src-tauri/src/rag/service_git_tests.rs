use super::*;

#[test]
fn source_sync_skips_a_url_with_multiple_branch_values() {
    let decision = git_source_sync_branch([None, Some("feature".to_string())], || {
        Ok(Some("main".to_string()))
    });

    assert_eq!(decision, GitSourceSyncBranch::SkipAmbiguous);
}

#[test]
fn source_sync_skips_conflicting_http_aliases_of_the_same_clone() {
    let http_url = "http://example.invalid/owner/repo.git".to_string();
    let https_url = "https://example.invalid/owner/repo.git".to_string();
    let persistent = std::env::temp_dir().join(format!("rag-git-alias-{}", Uuid::new_v4()));
    let http_path = persistent.join("docs").join("http-history.md");
    let https_path = persistent.join("docs").join("https-history.md");
    let unrelated = std::env::temp_dir().join(format!("rag-git-unrelated-{}", Uuid::new_v4()));
    let unrelated_path = unrelated.join("docs").join("other.md");
    let recorded_paths = [
        (http_url.as_str(), http_path.as_path()),
        (https_url.as_str(), https_path.as_path()),
        ("https://example.invalid/other.git", unrelated_path.as_path()),
    ];
    let selected_group = git_source_group_for_recorded_path(
        recorded_paths,
        &http_path,
        |path| {
            if path.starts_with(&persistent) {
                Some(persistent.clone())
            } else if path.starts_with(&unrelated) {
                Some(unrelated.clone())
            } else {
                None
            }
        },
    )
    .unwrap();

    assert_eq!(
        selected_group.urls,
        vec![http_url.clone(), https_url.clone()]
    );
    let decision = git_source_sync_branch_for_group(
        &selected_group.urls,
        [
            (http_url.as_str(), Some("main".to_string())),
            (https_url.as_str(), Some("feature".to_string())),
        ],
        || panic!("conflicting aliases must skip before reading clone HEAD"),
    );

    assert_eq!(decision, GitSourceSyncBranch::SkipAmbiguous);
}

#[test]
fn original_update_selects_the_group_containing_its_recorded_path() {
    let first = std::env::temp_dir().join(format!("rag-git-first-{}", Uuid::new_v4()));
    let recorded = std::env::temp_dir().join(format!("rag-git-recorded-{}", Uuid::new_v4()));
    let selected_path = recorded.join("docs").join("selected.md");
    let other_path = first.join("docs").join("other.md");
    let selected = git_source_group_for_recorded_path(
        [
            ("https://example.invalid/first.git", other_path.as_path()),
            (
                "https://example.invalid/recorded.git",
                selected_path.as_path(),
            ),
        ],
        &selected_path,
        |path| {
            if path.starts_with(&first) {
                Some(first.clone())
            } else if path.starts_with(&recorded) {
                Some(recorded.clone())
            } else {
                None
            }
        },
    )
    .unwrap();
    assert_eq!(
        selected.urls,
        vec!["https://example.invalid/recorded.git".to_string()]
    );
    assert!(git_source_group_for_recorded_path(
        [("https://example.invalid/recorded.git", selected_path.as_path())],
        &std::env::temp_dir().join("rag-git-absent"),
        |_| None,
    )
    .is_none());
}

#[test]
fn original_update_guard_rejects_default_and_explicit_alias_mix() {
    let http_url = "http://example.invalid/owner/repo.git".to_string();
    let https_url = "https://example.invalid/owner/repo.git".to_string();
    let persistent = std::env::temp_dir().join(format!("rag-git-alias-{}", Uuid::new_v4()));
    let groups = group_git_source_urls([
        (http_url.clone(), persistent.clone()),
        (https_url.clone(), persistent),
    ]);

    assert_eq!(groups.len(), 1);
    assert!(
        !git_source_group_branch_safe_for_original_update(
            &groups[0].urls,
            [
                (http_url.as_str(), None),
                (https_url.as_str(), Some("feature".to_string())),
            ],
            || panic!("ambiguous historical branch metadata must fail closed"),
        ),
        "original updates must not read whichever branch happens to be checked out"
    );
}

#[test]
fn same_branch_http_aliases_share_one_scan_and_add_path() {
    let http_url = "http://example.invalid/owner/repo.git".to_string();
    let https_url = "https://example.invalid/owner/repo.git".to_string();
    let persistent = std::env::temp_dir().join(format!("rag-git-alias-{}", Uuid::new_v4()));
    let recorded_path = persistent.join("docs").join("historical.md");
    let groups = group_git_source_urls_with_recorded_paths(
        [(https_url.clone(), persistent.clone())],
        [(http_url.as_str(), recorded_path.as_path())],
        |path| (path == recorded_path).then(|| persistent.clone()),
    );

    assert_eq!(groups.len(), 1, "aliases must result in one clone scan");
    assert_eq!(
        groups[0].urls.first().map(String::as_str),
        Some(http_url.as_str()),
        "added docs should retain a stable raw-URL provenance"
    );
    let decision = git_source_sync_branch_for_group(
        &groups[0].urls,
        [
            (http_url.as_str(), Some("main".to_string())),
            (https_url.as_str(), Some("main".to_string())),
        ],
        || Ok(Some("main".to_string())),
    );
    assert_eq!(
        decision,
        GitSourceSyncBranch::Sync(Some("main".to_string()))
    );

    let referenced = std::collections::HashSet::new();
    let additions = unique_unreferenced_git_paths(
        [("/clone/new.md", "docs"), ("/clone/new.md", "docs")],
        &referenced,
    );
    assert_eq!(additions, vec![("/clone/new.md", "docs")]);
}

#[test]
fn source_sync_skips_a_url_with_two_explicit_branches() {
    let decision = git_source_sync_branch(
        [Some("main".to_string()), Some("feature".to_string())],
        || Ok(Some("main".to_string())),
    );

    assert_eq!(decision, GitSourceSyncBranch::SkipAmbiguous);
}

#[test]
fn source_sync_accepts_a_single_explicit_branch_only_when_head_matches() {
    assert_eq!(
        git_source_sync_branch([Some("feature".to_string())], || {
            Ok(Some("feature".to_string()))
        }),
        GitSourceSyncBranch::Sync(Some("feature".to_string()))
    );
    assert_eq!(
        git_source_sync_branch([Some("feature".to_string())], || {
            Ok(Some("main".to_string()))
        }),
        GitSourceSyncBranch::SkipCloneBranchMismatch(
            "recorded branch feature differs from clone HEAD main".to_string()
        )
    );
}

#[test]
fn source_sync_skips_explicit_branch_when_clone_head_is_missing_or_unreadable() {
    assert_eq!(
        git_source_sync_branch([Some("feature".to_string())], || Ok(None)),
        GitSourceSyncBranch::SkipCloneBranchMismatch(
            "clone HEAD has no identifiable branch (recorded feature)".to_string()
        )
    );
    assert_eq!(
        git_source_sync_branch([Some("feature".to_string())], || {
            Err(anyhow::anyhow!("cannot read clone HEAD"))
        }),
        GitSourceSyncBranch::SkipCloneBranchMismatch(
            "clone HEAD could not be read (recorded feature): cannot read clone HEAD".to_string()
        )
    );
}

#[test]
fn source_sync_allows_the_single_remote_default_branch_without_head_name_validation() {
    assert_eq!(
        git_source_sync_branch([None], || {
            panic!("default branch does not require explicit-name validation")
        }),
        GitSourceSyncBranch::Sync(None)
    );
}
