import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:url_launcher/url_launcher.dart';
import '../services/event_service.dart';
import '../services/specials_service.dart';
import '../services/user_preferences_service.dart';
import '../models/event.dart';
import '../models/app_region.dart';
import '../widgets/specials_banner_widget.dart';

class EventsScreen extends StatefulWidget {
  const EventsScreen({super.key});

  @override
  State<EventsScreen> createState() => _EventsScreenState();
}

class _EventsScreenState extends State<EventsScreen> {
  String? _selectedSource;

  // ---------------------------------------------------------------------------
  // Filtering
  // ---------------------------------------------------------------------------

  List<Event> _filteredEvents(
      List<Event> all, UserPreferencesService prefs) {
    List<Event> events = all;

    // Source filter
    if (_selectedSource != null) {
      events = events.where((e) => e.source == _selectedSource).toList();
    }

    // Region filter — events with no matching county are always shown
    if (prefs.isFilteringByRegion) {
      events = events.where((e) {
        final region = e.region;
        if (region == null) return true;
        return prefs.selectedRegions.contains(region);
      }).toList();
    }

    return events;
  }

  // ---------------------------------------------------------------------------
  // Date grouping helpers
  // ---------------------------------------------------------------------------

  Map<String, List<Event>> _groupByDate(List<Event> events) {
    final Map<String, List<Event>> grouped = {};
    for (final e in events) {
      final d = e.dateObject;
      final key = d != null
          ? '${d.year}-${d.month.toString().padLeft(2, '0')}-${d.day.toString().padLeft(2, '0')}'
          : 'Unknown Date';
      grouped.putIfAbsent(key, () => []).add(e);
    }
    return grouped;
  }

  String _formatDateHeader(String key) {
    if (key == 'Unknown Date') return 'Date Unknown';
    final d = DateTime.tryParse(key);
    if (d == null) return key;
    const weekdays = [
      '', 'Monday', 'Tuesday', 'Wednesday', 'Thursday',
      'Friday', 'Saturday', 'Sunday',
    ];
    const months = [
      '', 'January', 'February', 'March', 'April', 'May', 'June',
      'July', 'August', 'September', 'October', 'November', 'December',
    ];
    final now = DateTime.now();
    final today = DateTime(now.year, now.month, now.day);
    final tomorrow = today.add(const Duration(days: 1));
    final eventDay = DateTime(d.year, d.month, d.day);

    if (eventDay == today) return 'Today — ${months[d.month]} ${d.day}';
    if (eventDay == tomorrow) return 'Tomorrow — ${months[d.month]} ${d.day}';
    return '${weekdays[d.weekday]}, ${months[d.month]} ${d.day}';
  }

  // ---------------------------------------------------------------------------
  // Build
  // ---------------------------------------------------------------------------

  @override
  Widget build(BuildContext context) {
    return Consumer3<EventService, SpecialsService, UserPreferencesService>(
      builder: (context, eventSvc, specialsSvc, prefs, _) {
        final activeSpecials = specialsSvc.activeSpecials;
        final events = _filteredEvents(eventSvc.events, prefs);
        final grouped = _groupByDate(events);
        final dateKeys = grouped.keys.toList();

        final bool hasFilter =
            _selectedSource != null || prefs.isFilteringByRegion;

        return Scaffold(
          backgroundColor: Colors.grey.shade100,
          body: RefreshIndicator(
            onRefresh: () async {
              await Future.wait([
                eventSvc.fetchEvents(),
                specialsSvc.fetchSpecials(),
              ]);
            },
            child: CustomScrollView(
              slivers: [
                // App bar
                SliverAppBar(
                  pinned: true,
                  backgroundColor: const Color(0xFF0D3A1A),
                  foregroundColor: Colors.white,
                  title: const Text(
                    'High Country Events',
                    style: TextStyle(
                      color: Colors.white,
                      fontWeight: FontWeight.bold,
                      fontSize: 17,
                    ),
                  ),
                  actions: [
                    // Filter button
                    IconButton(
                      icon: Stack(
                        clipBehavior: Clip.none,
                        children: [
                          Icon(
                            hasFilter
                                ? Icons.filter_list
                                : Icons.filter_list_outlined,
                            color: Colors.white,
                          ),
                          if (hasFilter)
                            Positioned(
                              right: -2,
                              top: -2,
                              child: Container(
                                width: 8,
                                height: 8,
                                decoration: const BoxDecoration(
                                  color: Colors.amber,
                                  shape: BoxShape.circle,
                                ),
                              ),
                            ),
                        ],
                      ),
                      onPressed: () => _showFilterSheet(
                          context, eventSvc, prefs),
                    ),
                    // Refresh
                    IconButton(
                      icon: const Icon(Icons.refresh, color: Colors.white),
                      onPressed: eventSvc.isLoading
                          ? null
                          : () async {
                              await Future.wait([
                                eventSvc.fetchEvents(),
                                specialsSvc.fetchSpecials(),
                              ]);
                            },
                    ),
                  ],
                ),

                // Specials banner
                if (activeSpecials.isNotEmpty)
                  SliverToBoxAdapter(
                    child: SpecialsBannerWidget(
                        activeSpecials: activeSpecials),
                  ),

                // Event count label
                SliverToBoxAdapter(
                  child: Padding(
                    padding: const EdgeInsets.fromLTRB(16, 12, 16, 4),
                    child: Row(
                      children: [
                        Text(
                          'Events (${events.length})',
                          style: TextStyle(
                            fontSize: 13,
                            fontWeight: FontWeight.w600,
                            color: Colors.grey.shade600,
                          ),
                        ),
                        if (_selectedSource != null) ...[
                          const SizedBox(width: 8),
                          _SourceChip(
                            label: _selectedSource!,
                            onRemove: () =>
                                setState(() => _selectedSource = null),
                          ),
                        ],
                      ],
                    ),
                  ),
                ),

                // Loading
                if (eventSvc.isLoading)
                  const SliverToBoxAdapter(
                    child: Padding(
                      padding: EdgeInsets.all(40),
                      child: Center(child: CircularProgressIndicator()),
                    ),
                  )
                // Error
                else if (eventSvc.errorMessage != null && events.isEmpty)
                  SliverToBoxAdapter(
                    child: _EmptyState(
                      icon: Icons.cloud_off_outlined,
                      message: eventSvc.errorMessage!,
                    ),
                  )
                // Empty
                else if (events.isEmpty)
                  const SliverToBoxAdapter(
                    child: _EmptyState(
                      icon: Icons.event_busy_outlined,
                      message: 'No events found for the selected filters.',
                    ),
                  )
                // Event list grouped by date
                else
                  SliverList(
                    delegate: SliverChildBuilderDelegate(
                      (context, sectionIndex) {
                        final key = dateKeys[sectionIndex];
                        final sectionEvents = grouped[key]!;
                        return _DateSection(
                          header: _formatDateHeader(key),
                          events: sectionEvents,
                          onSourceTap: (src) =>
                              setState(() => _selectedSource = src),
                        );
                      },
                      childCount: dateKeys.length,
                    ),
                  ),

                const SliverToBoxAdapter(child: SizedBox(height: 24)),
              ],
            ),
          ),
        );
      },
    );
  }

  // ---------------------------------------------------------------------------
  // Filter sheet
  // ---------------------------------------------------------------------------

  void _showFilterSheet(BuildContext context, EventService eventSvc,
      UserPreferencesService prefs) {
    showModalBottomSheet(
      context: context,
      isScrollControlled: true,
      backgroundColor: Colors.transparent,
      builder: (_) => ChangeNotifierProvider.value(
        value: prefs,
        child: _FilterSheet(
          sources: eventSvc.allSources,
          selectedSource: _selectedSource,
          onSourceChanged: (src) {
            setState(() => _selectedSource = src);
            Navigator.pop(context);
          },
          onClearAll: () {
            setState(() => _selectedSource = null);
            Navigator.pop(context);
          },
        ),
      ),
    );
  }
}

// ---------------------------------------------------------------------------
// Date section
// ---------------------------------------------------------------------------

class _DateSection extends StatelessWidget {
  final String header;
  final List<Event> events;
  final ValueChanged<String> onSourceTap;

  const _DateSection({
    required this.header,
    required this.events,
    required this.onSourceTap,
  });

  @override
  Widget build(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Padding(
          padding: const EdgeInsets.fromLTRB(16, 14, 16, 6),
          child: Text(
            header,
            style: TextStyle(
              fontSize: 13,
              fontWeight: FontWeight.w700,
              color: Colors.grey.shade500,
              letterSpacing: 0.3,
            ),
          ),
        ),
        Container(
          margin: const EdgeInsets.symmetric(horizontal: 12),
          decoration: BoxDecoration(
            color: Colors.white,
            borderRadius: BorderRadius.circular(12),
            boxShadow: [
              BoxShadow(
                color: Colors.black.withOpacity(0.05),
                blurRadius: 4,
                offset: const Offset(0, 2),
              ),
            ],
          ),
          child: Column(
            children: [
              for (int i = 0; i < events.length; i++) ...[
                _EventRow(event: events[i], onSourceTap: onSourceTap),
                if (i < events.length - 1)
                  Divider(
                    height: 1,
                    thickness: 1,
                    indent: 16,
                    color: Colors.grey.shade100,
                  ),
              ],
            ],
          ),
        ),
      ],
    );
  }
}

// ---------------------------------------------------------------------------
// Event row
// ---------------------------------------------------------------------------

class _EventRow extends StatelessWidget {
  final Event event;
  final ValueChanged<String> onSourceTap;

  const _EventRow({required this.event, required this.onSourceTap});

  @override
  Widget build(BuildContext context) {
    final dt = event.dateObject;
    final timeStr = dt != null ? _formatTime(dt) : null;
    final region = event.region;

    return InkWell(
      onTap: event.url.isNotEmpty ? () => _launchUrl(event.url) : null,
      borderRadius: BorderRadius.circular(12),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Time column
            SizedBox(
              width: 52,
              child: timeStr != null
                  ? Text(
                      timeStr,
                      style: TextStyle(
                        fontSize: 11,
                        fontWeight: FontWeight.w600,
                        color: Colors.grey.shade500,
                      ),
                    )
                  : const SizedBox.shrink(),
            ),

            const SizedBox(width: 8),

            // Content
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    event.title,
                    style: const TextStyle(
                      fontSize: 14,
                      fontWeight: FontWeight.w600,
                      color: Colors.black87,
                    ),
                    maxLines: 2,
                    overflow: TextOverflow.ellipsis,
                  ),
                  const SizedBox(height: 3),
                  Row(
                    children: [
                      Icon(Icons.location_on_outlined,
                          size: 12, color: Colors.grey.shade400),
                      const SizedBox(width: 2),
                      Expanded(
                        child: Text(
                          event.location,
                          style: TextStyle(
                            fontSize: 12,
                            color: Colors.grey.shade500,
                          ),
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                        ),
                      ),
                    ],
                  ),
                  if (event.description.isNotEmpty) ...[
                    const SizedBox(height: 3),
                    Text(
                      event.description,
                      style: TextStyle(
                        fontSize: 12,
                        color: Colors.grey.shade500,
                        height: 1.3,
                      ),
                      maxLines: 2,
                      overflow: TextOverflow.ellipsis,
                    ),
                  ],
                  const SizedBox(height: 6),
                  // Tags row
                  Row(
                    children: [
                      // Source chip
                      GestureDetector(
                        onTap: () => onSourceTap(event.source),
                        child: Container(
                          padding: const EdgeInsets.symmetric(
                              horizontal: 7, vertical: 2),
                          decoration: BoxDecoration(
                            color: Colors.grey.shade100,
                            borderRadius: BorderRadius.circular(10),
                          ),
                          child: Text(
                            event.source,
                            style: TextStyle(
                              fontSize: 10,
                              color: Colors.grey.shade600,
                              fontWeight: FontWeight.w500,
                            ),
                          ),
                        ),
                      ),
                      // Region badge
                      if (region != null) ...[
                        const SizedBox(width: 5),
                        Container(
                          padding: const EdgeInsets.symmetric(
                              horizontal: 7, vertical: 2),
                          decoration: BoxDecoration(
                            color: region.color.withOpacity(0.1),
                            borderRadius: BorderRadius.circular(10),
                          ),
                          child: Text(
                            region.shortName,
                            style: TextStyle(
                              fontSize: 10,
                              color: region.color,
                              fontWeight: FontWeight.w600,
                            ),
                          ),
                        ),
                      ],
                    ],
                  ),
                ],
              ),
            ),

            // Link icon
            if (event.url.isNotEmpty)
              Padding(
                padding: const EdgeInsets.only(left: 6),
                child: Icon(Icons.open_in_new,
                    size: 14, color: Colors.grey.shade300),
              ),
          ],
        ),
      ),
    );
  }

  String _formatTime(DateTime dt) {
    final h = dt.hour == 0
        ? 12
        : dt.hour > 12
            ? dt.hour - 12
            : dt.hour;
    final m = dt.minute.toString().padLeft(2, '0');
    final ampm = dt.hour < 12 ? 'AM' : 'PM';
    if (dt.minute == 0) return '$h $ampm';
    return '$h:$m $ampm';
  }

  Future<void> _launchUrl(String url) async {
    final uri = Uri.parse(url);
    if (await canLaunchUrl(uri)) {
      await launchUrl(uri, mode: LaunchMode.externalApplication);
    }
  }
}

// ---------------------------------------------------------------------------
// Filter sheet
// ---------------------------------------------------------------------------

class _FilterSheet extends StatelessWidget {
  final List<String> sources;
  final String? selectedSource;
  final ValueChanged<String?> onSourceChanged;
  final VoidCallback onClearAll;

  const _FilterSheet({
    required this.sources,
    required this.selectedSource,
    required this.onSourceChanged,
    required this.onClearAll,
  });

  @override
  Widget build(BuildContext context) {
    return Consumer<UserPreferencesService>(
      builder: (context, prefs, _) {
        return Container(
          decoration: const BoxDecoration(
            color: Colors.white,
            borderRadius: BorderRadius.vertical(top: Radius.circular(20)),
          ),
          padding: EdgeInsets.only(
            bottom: MediaQuery.of(context).viewInsets.bottom + 24,
          ),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              // Handle
              Center(
                child: Container(
                  margin: const EdgeInsets.only(top: 12, bottom: 16),
                  width: 40,
                  height: 4,
                  decoration: BoxDecoration(
                    color: Colors.grey.shade300,
                    borderRadius: BorderRadius.circular(2),
                  ),
                ),
              ),

              // Header
              Padding(
                padding: const EdgeInsets.symmetric(horizontal: 20),
                child: Row(
                  children: [
                    const Text(
                      'Filter Events',
                      style: TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.w800,
                      ),
                    ),
                    const Spacer(),
                    if (selectedSource != null || prefs.isFilteringByRegion)
                      TextButton(
                        onPressed: () {
                          for (final r in AppRegion.values) {
                            if (!prefs.selectedRegions.contains(r)) {
                              prefs.toggleRegion(r);
                            }
                          }
                          onClearAll();
                        },
                        child: const Text('Clear All'),
                      ),
                  ],
                ),
              ),

              const SizedBox(height: 8),

              // Counties section
              const Padding(
                padding: EdgeInsets.fromLTRB(20, 8, 20, 8),
                child: Text(
                  'COUNTIES',
                  style: TextStyle(
                    fontSize: 11,
                    fontWeight: FontWeight.w700,
                    color: Colors.grey,
                    letterSpacing: 0.8,
                  ),
                ),
              ),

              Padding(
                padding: const EdgeInsets.symmetric(horizontal: 16),
                child: Column(
                  children: AppRegion.values.map((region) {
                    final selected = prefs.selectedRegions.contains(region);
                    return ListTile(
                      dense: true,
                      contentPadding:
                          const EdgeInsets.symmetric(horizontal: 4),
                      leading: Container(
                        width: 32,
                        height: 32,
                        decoration: BoxDecoration(
                          color: region.color.withOpacity(0.12),
                          shape: BoxShape.circle,
                        ),
                        child: Icon(region.icon,
                            size: 15, color: region.color),
                      ),
                      title: Text(
                        region.shortName,
                        style: const TextStyle(
                          fontSize: 14,
                          fontWeight: FontWeight.w600,
                        ),
                      ),
                      subtitle: Text(
                        region.rawValue,
                        style: TextStyle(
                          fontSize: 11,
                          color: Colors.grey.shade500,
                        ),
                      ),
                      trailing: Icon(
                        selected
                            ? Icons.check_box
                            : Icons.check_box_outline_blank,
                        color: selected ? region.color : Colors.grey.shade400,
                      ),
                      onTap: () => prefs.toggleRegion(region),
                    );
                  }).toList(),
                ),
              ),

              // Source filter
              if (sources.isNotEmpty) ...[
                const Padding(
                  padding: EdgeInsets.fromLTRB(20, 12, 20, 8),
                  child: Text(
                    'SOURCE',
                    style: TextStyle(
                      fontSize: 11,
                      fontWeight: FontWeight.w700,
                      color: Colors.grey,
                      letterSpacing: 0.8,
                    ),
                  ),
                ),
                SizedBox(
                  height: 38,
                  child: ListView(
                    scrollDirection: Axis.horizontal,
                    padding:
                        const EdgeInsets.symmetric(horizontal: 16),
                    children: [
                      _SourcePill(
                        label: 'All',
                        selected: selectedSource == null,
                        onTap: () => onSourceChanged(null),
                      ),
                      ...sources.map((src) => Padding(
                            padding: const EdgeInsets.only(left: 8),
                            child: _SourcePill(
                              label: src,
                              selected: selectedSource == src,
                              onTap: () => onSourceChanged(src),
                            ),
                          )),
                    ],
                  ),
                ),
              ],

              const SizedBox(height: 8),
            ],
          ),
        );
      },
    );
  }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

class _SourcePill extends StatelessWidget {
  final String label;
  final bool selected;
  final VoidCallback onTap;

  const _SourcePill(
      {required this.label, required this.selected, required this.onTap});

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: onTap,
      child: AnimatedContainer(
        duration: const Duration(milliseconds: 150),
        padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 6),
        decoration: BoxDecoration(
          color: selected ? const Color(0xFF0D3A1A) : Colors.grey.shade100,
          borderRadius: BorderRadius.circular(20),
          border: Border.all(
            color: selected
                ? const Color(0xFF0D3A1A)
                : Colors.grey.shade300,
          ),
        ),
        child: Text(
          label,
          style: TextStyle(
            fontSize: 12,
            fontWeight: FontWeight.w600,
            color: selected ? Colors.white : Colors.grey.shade700,
          ),
        ),
      ),
    );
  }
}

class _SourceChip extends StatelessWidget {
  final String label;
  final VoidCallback onRemove;

  const _SourceChip({required this.label, required this.onRemove});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 3),
      decoration: BoxDecoration(
        color: const Color(0xFF0D3A1A).withOpacity(0.1),
        borderRadius: BorderRadius.circular(12),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Text(
            label,
            style: const TextStyle(
              fontSize: 11,
              color: Color(0xFF0D3A1A),
              fontWeight: FontWeight.w600,
            ),
          ),
          const SizedBox(width: 4),
          GestureDetector(
            onTap: onRemove,
            child: const Icon(Icons.close,
                size: 13, color: Color(0xFF0D3A1A)),
          ),
        ],
      ),
    );
  }
}

class _EmptyState extends StatelessWidget {
  final IconData icon;
  final String message;

  const _EmptyState({required this.icon, required this.message});

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 60, horizontal: 32),
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, size: 48, color: Colors.grey.shade300),
          const SizedBox(height: 12),
          Text(
            message,
            textAlign: TextAlign.center,
            style: TextStyle(color: Colors.grey.shade500, fontSize: 14),
          ),
        ],
      ),
    );
  }
}
