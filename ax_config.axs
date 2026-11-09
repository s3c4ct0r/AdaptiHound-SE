function InitService() {
    let action = menu.create_action("AdaptiHound-SE: Merge parts", function(files) {
        if (!files || files.length !== 1 || !/(?:\.manifest\.json|-part[0-9]{6,}\.(?:zip|json))$/.test(files[0].path || "")) {
            ax.show_message("AdaptiHound-SE", "Select any finished AdaptiHound ZIP/JSON part or its manifest in Downloads.");
            return;
        }
        ax.service_command("AdaptiHoundSE", "merge", {download_id: files[0].file_id});
    });
    menu.add_downloads_finished(action, ["beacon", "gopher"], [], []);
}

function data_handler(data) {
    let result;
    try { result = JSON.parse(data); }
    catch (_) { ax.show_message("AdaptiHound-SE", "The server returned an invalid archive response."); return; }
    if (result.phase === "queued") return;
    let text = result.message || "Archive operation finished.";
    if (result.filename) text += "\n\n" + result.filename;
    if (result.phase === "complete" || result.phase === "warning") {
        text += "\nRecords: " + result.records + "; parts: " + result.parts + "; ZIP entries: " + result.entries;
    }
    ax.show_message("AdaptiHound-SE", text);
}
