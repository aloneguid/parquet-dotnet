<script>
    (function () {

    const document = window.document;

// helper utils

    function generateGuid() {
    let result, i, j;
    result = '';
    for(j=0; j<32; j++) {
    if( j === 8 || j === 12 || j === 16 || j === 20)
    result = result + '-';
    i = Math.floor(Math.random()*16).toString(16).toUpperCase();
    result = result + i;
}
    return result;
}

    function setCookie(name, value, days) {
    let expires = "";
    if (days) {
    const date = new Date();
    date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
    expires = "; expires=" + date.toUTCString();
}
    document.cookie = name + "=" + (value || "") + expires + "; path=/";
}

    function getCookie(name) {
    const nameEQ = name + "=";
    const ca = document.cookie.split(';');
    for (var i = 0; i < ca.length; i++) {
    var c = ca[i];
    while (c.charAt(0) == ' ') c = c.substring(1, c.length);
    if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length, c.length);
}
    return null;
}

    // end of helper utils

    //minify me!
    const location = window.location;
    const payload = {};
    payload.u = location.href
    payload.r = document.referrer
    payload.w = window.innerWidth
    payload.h = window.innerHeight
    payload.sid = generateGuid()

    // set client id
    let iid = getCookie("clientId");
    if(iid == null) {
    iid = generateGuid()
    setCookie("clientId", iid, 365);
}
    payload.iid = iid;

    function report(eventName, extUrl) {
    var request = new XMLHttpRequest();
    request.open("POST", "https://alt.aloneguid.uk/events?key=pqdoc", true);
    request.setRequestHeader("Content-Type", "application/json");
    payload.e = eventName
    if(extUrl) payload.x = extUrl
    request.send(JSON.stringify(payload));

    console.log(payload)
}

    function handleClick(event) {
    var link = event.target
    var click = event.type === 'click'
    while (link && (typeof link.tagName === 'undefined' || link.tagName.toLowerCase() !== 'a' || !link.href)) {
    link = link.parentNode
}

    if (link && link.href) {

    if(click) {
    report("click", link.href)
}

    // delay so we are notified
    if (!link.target || link.target.match(/^_(self|parent|top)$/i)) {
    if (!(event.ctrlKey || event.metaKey || event.shiftKey) && click) {
    setTimeout(function () {
    location.href = link.href;
}, 150);
    event.preventDefault();
}
}
}

    console.log(event)
}

    report("load");
})();
</script>