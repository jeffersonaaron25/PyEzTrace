/*
  Ensure "Copy page" always copies the current page markdown.
  This intercepts mkdocs-copy-to-llm's click handler and resolves the
  markdown URL from the current page's "View source of this page" button.
*/
(function () {
  "use strict";
  const markdownCache = new Map();

  function removeFrontMatter(markdown) {
    if (!markdown.startsWith("---")) {
      return markdown.trim();
    }
    const lines = markdown.split("\n");
    let end = -1;
    for (let i = 1; i < lines.length; i += 1) {
      if (lines[i].trim() === "---") {
        end = i;
        break;
      }
    }
    if (end === -1) {
      return markdown.trim();
    }
    return lines.slice(end + 1).join("\n").trim();
  }

  function getRawRepoBaseUrl() {
    const metaRepo = document.querySelector('meta[name="mkdocs-copy-to-llm-repo-url"]');
    if (metaRepo && metaRepo.content) {
      return metaRepo.content.replace(/\/+$/, "");
    }
    return "https://raw.githubusercontent.com/jeffersonaaron25/pyeztrace/main";
  }

  function stripBasePath(pathname) {
    const metaBasePath = document.querySelector('meta[name="mkdocs-copy-to-llm-base-path"]');
    if (!metaBasePath || !metaBasePath.content) {
      return pathname;
    }

    const basePath = metaBasePath.content.startsWith("/")
      ? metaBasePath.content
      : "/" + metaBasePath.content;
    const normalizedBase = basePath.replace(/\/+$/, "");

    if (pathname === normalizedBase) {
      return "/";
    }
    if (pathname.startsWith(normalizedBase + "/")) {
      return pathname.slice(normalizedBase.length);
    }
    return pathname;
  }

  function getMarkdownPathFromLocation() {
    let path = stripBasePath(window.location.pathname);
    path = path.split("?")[0].split("#")[0];
    path = path.replace(/^\/+/, "").replace(/\/+$/, "");

    if (!path) {
      return "index.md";
    }
    if (path.endsWith(".html")) {
      return path.replace(/\.html$/, ".md");
    }
    if (path.endsWith(".md")) {
      return path;
    }
    return path + ".md";
  }

  function getLocalMarkdownUrl() {
    let path = window.location.pathname.split("?")[0].split("#")[0];

    if (path.endsWith("/")) {
      path = path + "index.md";
    } else if (path.endsWith("/index.html")) {
      path = path.replace(/\/index\.html$/, "/index.md");
    } else if (path.endsWith(".html")) {
      path = path.replace(/\.html$/, ".md");
    } else if (!path.endsWith(".md")) {
      path = path + "/index.md";
    }

    return window.location.origin + path;
  }

  function normalizeGithubRawUrl(url) {
    if (!url) {
      return "";
    }
    // Convert GitHub "raw" page URL to raw.githubusercontent URL to avoid redirects.
    const match = url.match(
      /^https?:\/\/github\.com\/([^/]+)\/([^/]+)\/raw\/([^/]+)\/(.+)$/
    );
    if (match) {
      const owner = match[1];
      const repo = match[2];
      const branch = match[3];
      const filePath = match[4];
      return (
        "https://raw.githubusercontent.com/" +
        owner +
        "/" +
        repo +
        "/" +
        branch +
        "/" +
        filePath
      );
    }
    return url;
  }

  function copyPageMarkdownFromDom(button) {
    const contentRoot =
      button.closest(".md-content__inner") ||
      document.querySelector(".md-content__inner");
    if (!contentRoot) {
      return document.title;
    }

    const clone = contentRoot.cloneNode(true);
    clone
      .querySelectorAll(".md-clipboard, .copy-to-llm, .headerlink, script, style")
      .forEach((node) => node.remove());

    let html = clone.innerHTML;
    let text = html
      .replace(/<h1[^>]*>(.*?)<\/h1>/gis, "# $1\n\n")
      .replace(/<h2[^>]*>(.*?)<\/h2>/gis, "## $1\n\n")
      .replace(/<h3[^>]*>(.*?)<\/h3>/gis, "### $1\n\n")
      .replace(/<h4[^>]*>(.*?)<\/h4>/gis, "#### $1\n\n")
      .replace(/<pre[^>]*><code[^>]*>(.*?)<\/code><\/pre>/gis, "```\n$1\n```\n\n")
      .replace(/<code[^>]*>(.*?)<\/code>/gis, "`$1`")
      .replace(/<strong[^>]*>(.*?)<\/strong>/gis, "**$1**")
      .replace(/<em[^>]*>(.*?)<\/em>/gis, "*$1*")
      .replace(/<a[^>]*href="([^"]*)"[^>]*>(.*?)<\/a>/gis, "[$2]($1)")
      .replace(/<li[^>]*>(.*?)<\/li>/gis, "- $1\n")
      .replace(/<p[^>]*>(.*?)<\/p>/gis, "$1\n\n")
      .replace(/<br[^>]*>/gis, "\n")
      .replace(/<[^>]+>/g, "")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&amp;/g, "&");

    return text.replace(/\n{3,}/g, "\n\n").trim();
  }

  function getCurrentMarkdownUrl(button, options) {
    const opts = options || {};
    if (opts.preferLocal) {
      return getLocalMarkdownUrl();
    }

    const contentRoot = button && button.closest
      ? (button.closest(".md-content__inner") || document.querySelector(".md-content__inner"))
      : document.querySelector(".md-content__inner");

    const sourceLink = contentRoot
      ? contentRoot.querySelector('a.md-content__button[title*="View source"]')
      : null;
    if (sourceLink && sourceLink.href) {
      return normalizeGithubRawUrl(sourceLink.href);
    }

    const base = getRawRepoBaseUrl();
    const mdPath = getMarkdownPathFromLocation();
    return base + "/docs/" + mdPath;
  }

  async function fetchMarkdown(markdownUrl) {
    if (markdownCache.has(markdownUrl)) {
      return markdownCache.get(markdownUrl);
    }

    const response = await fetch(markdownUrl, { cache: "no-store" });
    if (!response.ok) {
      throw new Error("Failed to fetch markdown: " + response.status);
    }
    const markdown = removeFrontMatter(await response.text());
    markdownCache.set(markdownUrl, markdown);
    return markdown;
  }

  function fetchMarkdownSync(markdownUrl) {
    if (!markdownUrl) {
      return "";
    }
    try {
      const xhr = new XMLHttpRequest();
      xhr.open("GET", markdownUrl, false);
      xhr.send(null);
      if (xhr.status >= 200 && xhr.status < 300 && xhr.responseText) {
        const markdown = removeFrontMatter(xhr.responseText);
        markdownCache.set(markdownUrl, markdown);
        return markdown;
      }
    } catch (error) {
      // Fall through to other copy strategies.
    }
    return "";
  }

  async function prefetchCurrentPageMarkdown() {
    const markdownUrl = getCurrentMarkdownUrl(document.body, { preferLocal: true });
    if (!markdownUrl || markdownCache.has(markdownUrl)) {
      return;
    }
    try {
      await fetchMarkdown(markdownUrl);
    } catch (error) {
      // Keep silent; click handler will fallback.
    }
  }

  function closeDropdown(node) {
    const container = node.closest(".copy-to-llm-split-container");
    if (!container) {
      return;
    }
    const dropdown = container.querySelector(".copy-to-llm-dropdown");
    const dropdownButton = container.querySelector(".copy-to-llm-right");
    if (dropdown) {
      dropdown.classList.remove("show");
    }
    if (dropdownButton) {
      dropdownButton.classList.remove("active");
      dropdownButton.setAttribute("aria-expanded", "false");
      const chevron = dropdownButton.querySelector(".chevron-icon");
      if (chevron) {
        chevron.style.transform = "";
      }
    }
  }

  function tryExecCommandCopy(text) {
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    document.body.appendChild(textarea);
    textarea.select();
    const copied = document.execCommand("copy");
    document.body.removeChild(textarea);
    return copied;
  }

  async function writeClipboard(text) {
    // Prefer sync copy first to stay inside the click gesture in stricter browsers.
    if (tryExecCommandCopy(text)) {
      return;
    }

    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      return;
    }

    throw new Error("Clipboard copy failed");
  }

  function showToast(message) {
    const existing = document.querySelector(".copy-to-llm-toast");
    if (existing) {
      existing.remove();
    }

    const toast = document.createElement("div");
    toast.className = "copy-to-llm-toast show";
    toast.textContent = message;
    document.body.appendChild(toast);

    setTimeout(function () {
      toast.classList.remove("show");
      setTimeout(function () {
        toast.remove();
      }, 250);
    }, 2000);
  }

  function handleCopyPage(event, button) {
    event.preventDefault();
    event.stopPropagation();
    event.stopImmediatePropagation();

    const markdownUrl = getCurrentMarkdownUrl(button, { preferLocal: true });
    let output = "";

    if (markdownUrl && markdownCache.has(markdownUrl)) {
      output = markdownCache.get(markdownUrl);
    } else if (markdownUrl) {
      output = fetchMarkdownSync(markdownUrl);
    }

    if (!output) {
      output = copyPageMarkdownFromDom(button);
    }

    if (tryExecCommandCopy(output)) {
      showToast("Page markdown copied.");
      return;
    }

    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard
        .writeText(output)
        .then(function () {
          showToast("Page markdown copied.");
        })
        .catch(function (error) {
          console.error("[copy-page-fix] Clipboard failed:", error);
          showToast("Copy failed. Check clipboard permissions.");
        });
      return;
    }

    showToast("Copy failed. Check clipboard permissions.");
  }

  function openPrompt(target, markdownUrl) {
    const prompt = "Read " + markdownUrl + " so I can ask questions about it.";
    if (target === "chatgpt") {
      window.open(
        "https://chatgpt.com/?hints=search&q=" + encodeURIComponent(prompt),
        "_blank"
      );
      return;
    }
    if (target === "claude") {
      window.open(
        "https://claude.ai/new?q=" + encodeURIComponent(prompt),
        "_blank"
      );
    }
  }

  async function handleDropdownAction(event, item) {
    const action = item.dataset.action;
    if (!action) {
      return;
    }

    const handledActions = new Set([
      "copy-markdown-link",
      "view-markdown",
      "open-chatgpt",
      "open-claude",
    ]);
    if (!handledActions.has(action)) {
      return;
    }

    event.preventDefault();
    event.stopPropagation();
    event.stopImmediatePropagation();

    const markdownUrl = getCurrentMarkdownUrl(item);
    if (!markdownUrl) {
      closeDropdown(item);
      showToast("No markdown source URL found.");
      return;
    }

    try {
      if (action === "copy-markdown-link") {
        await writeClipboard(markdownUrl);
        closeDropdown(item);
        showToast("Markdown link copied.");
        return;
      }

      if (action === "view-markdown") {
        window.open(markdownUrl, "_blank");
        closeDropdown(item);
        return;
      }

      if (action === "open-chatgpt") {
        openPrompt("chatgpt", markdownUrl);
        closeDropdown(item);
        return;
      }

      if (action === "open-claude") {
        openPrompt("claude", markdownUrl);
        closeDropdown(item);
      }
    } catch (error) {
      closeDropdown(item);
      console.error("[copy-page-fix] Dropdown action failed:", error);
    }
  }

  document.addEventListener(
    "click",
    function (event) {
      const button = event.target.closest(".copy-to-llm-left.copy-to-llm-section");
      if (!button) {
        return;
      }
      handleCopyPage(event, button);
    },
    true
  );

  document.addEventListener(
    "click",
    function (event) {
      const item = event.target.closest(".copy-to-llm-dropdown-item");
      if (!item) {
        return;
      }
      void handleDropdownAction(event, item);
    },
    true
  );

  // Warm markdown cache on initial load and Material instant-navigation changes.
  if (typeof document$ !== "undefined" && document$.subscribe) {
    document$.subscribe(function () {
      void prefetchCurrentPageMarkdown();
    });
  } else if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      void prefetchCurrentPageMarkdown();
    });
  } else {
    void prefetchCurrentPageMarkdown();
  }
})();
