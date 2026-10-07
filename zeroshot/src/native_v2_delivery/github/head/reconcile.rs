use super::*;

pub(crate) async fn reconcile(
    authority: &GhCliDeliveryAuthority,
    request: ForgeHeadReconciliation<'_>,
    credential: ForgeCredential<'_>,
) -> Result<ForgeReconciliationOutcome, ForgeAuthorityError> {
    require_identity(request.published, request.observed)?;
    let context = HeadUpdateContext {
        authority,
        workspace: request.workspace,
        credential,
    };
    fetch_head(context, request.published, request.observed).await?;
    if !request.adopting_existing
        && !is_ancestor(
            context,
            &request.published.head_revision,
            &request.observed.head_revision,
        )
        .await?
    {
        return Ok(ForgeReconciliationOutcome::Refused(format!(
            "remote history was rewritten: published head {}; observed head {}; local work was preserved",
            request.published.head_revision, request.observed.head_revision,
        )));
    }
    reconcile_workspace(context, request).await
}

async fn reconcile_adopted_delivery(
    context: HeadUpdateContext<'_>,
    request: ForgeHeadReconciliation<'_>,
    local_head: String,
) -> Result<ForgeReconciliationOutcome, ForgeAuthorityError> {
    if !is_ancestor(context, &local_head, &request.observed.head_revision).await? {
        return Ok(ForgeReconciliationOutcome::Refused(format!(
            "existing remote head {} does not descend from retained local head {}; local work was preserved",
            request.observed.head_revision, local_head,
        )));
    }
    let mut anchor = request.observed.clone();
    anchor.head_revision = local_head;
    Box::pin(reconcile_workspace(
        context,
        ForgeHeadReconciliation {
            published: &anchor,
            adopting_existing: false,
            ..request
        },
    ))
    .await
}

fn require_identity(
    published: &ForgeReviewReceipt,
    observed: &ForgeReviewReceipt,
) -> Result<(), ForgeAuthorityError> {
    let bound_review_changed =
        !published.review_id.is_empty() && published.review_id != observed.review_id;
    if bound_review_changed
        || published.repository != observed.repository
        || published.target_branch != observed.target_branch
        || published.head_branch != observed.head_branch
    {
        return Err(ForgeAuthorityError::identity(format!(
            "published identity {published:?}; observed identity {observed:?}",
        )));
    }
    Ok(())
}

fn authorizes_fast_forward(request: &ForgeHeadReconciliation<'_>, head: &str, dirty: bool) -> bool {
    request.authorized_update && !dirty && head == request.published.head_revision
}

async fn reconcile_workspace(
    context: HeadUpdateContext<'_>,
    request: ForgeHeadReconciliation<'_>,
) -> Result<ForgeReconciliationOutcome, ForgeAuthorityError> {
    let git = context.authority.workspace_git();
    let (head, dirty) = git
        .workspace_state(context.workspace)
        .await
        .map_err(git_error)?;
    if is_ancestor(context, &request.observed.head_revision, &head).await? {
        return Ok(ForgeReconciliationOutcome::Unchanged);
    }
    if request.adopting_existing {
        return reconcile_adopted_delivery(context, request, head).await;
    }
    if !is_ancestor(context, &request.published.head_revision, &head).await? {
        return Ok(ForgeReconciliationOutcome::Refused(format!(
            "local history no longer contains published head {}; local head {head}; \
             observed head {}; work was preserved",
            request.published.head_revision, request.observed.head_revision,
        )));
    }
    if dirty {
        git.prepare_revision(
            context.workspace,
            &request.published.head_revision,
            request.commit_message,
        )
        .await
        .map_err(git_error)?;
    }
    let authorized = authorizes_fast_forward(&request, &head, dirty);
    integrate(context, request, authorized).await
}

async fn integrate(
    context: HeadUpdateContext<'_>,
    request: ForgeHeadReconciliation<'_>,
    authorized: bool,
) -> Result<ForgeReconciliationOutcome, ForgeAuthorityError> {
    let mut command = git_command(
        &context.authority.config,
        context.workspace,
        context.credential,
    );
    command.args([
        "-c",
        "user.name=Zeroshot",
        "-c",
        "user.email=delivery@zeroshot.invalid",
        "merge",
        "--no-edit",
    ]);
    if authorized {
        command.arg("--ff-only");
    }
    command.arg(&request.observed.head_revision);
    let output = capture(&mut command, context.authority.config.api_deadline).await?;
    let diagnostic = format!(
        "trusted delivery fetched remote head {} and reconciled published head {} with local work; \
         inspect the resulting workspace before another delivery\n{output}",
        request.observed.head_revision, request.published.head_revision,
    );
    if output.exit_status == Some(0) {
        return Ok(if authorized {
            ForgeReconciliationOutcome::Adopted
        } else {
            ForgeReconciliationOutcome::NeedsWork(diagnostic)
        });
    }
    if has_conflicts(context).await? {
        return Ok(ForgeReconciliationOutcome::NeedsWork(diagnostic));
    }
    Err(output.into())
}

async fn has_conflicts(context: HeadUpdateContext<'_>) -> Result<bool, ForgeAuthorityError> {
    let output = git_output(
        git_command(
            &context.authority.config,
            context.workspace,
            context.credential,
        )
        .args(["diff", "--name-only", "--diff-filter=U"]),
        context.authority.config.api_deadline,
    )
    .await?;
    Ok(!output.is_empty())
}

async fn is_ancestor(
    context: HeadUpdateContext<'_>,
    ancestor: &str,
    descendant: &str,
) -> Result<bool, ForgeAuthorityError> {
    let mut command = git_command(
        &context.authority.config,
        context.workspace,
        context.credential,
    );
    command.args(["merge-base", "--is-ancestor", ancestor, descendant]);
    let output = capture(&mut command, context.authority.config.api_deadline).await?;
    if output.exit_status == Some(1) {
        return Ok(false);
    }
    output.require_success()?;
    Ok(true)
}
